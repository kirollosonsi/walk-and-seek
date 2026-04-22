using System.Globalization;
using System.Text.Json;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Net;

var url = Environment.GetEnvironmentVariable("NATS_URL") ?? "nats://localhost:4222";

Log.Banner("NATS TESTER");
Log.Kv("NATS_URL", url);
Log.Kv("mode", args.FirstOrDefault() ?? "all");

await using var nats = new NatsClient(url);
Log.Ok($"connected to {url}");

var js = nats.CreateJetStreamContext();
Log.Ok("JetStream context created");

var mode = args.FirstOrDefault() ?? "all";
using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); Log.Warn("cancel requested, stopping…"); };

switch (mode)
{
	case "bootstrap": await Bootstrap(js, cts.Token); break;
	case "publish": await Publish(js, cts.Token); break;
	case "schedule": await PublishScheduled(js, cts.Token); break;
	case "consume": await Consume(js, cts.Token); break;
	case "dlq": await WatchDlq(nats, js, cts.Token); break;
	case "all":
		await Bootstrap(js, cts.Token);
		await Publish(js, cts.Token);
		await PublishScheduled(js, cts.Token);
		Log.Section("DLQ WATCHER (background)");
		_ = Task.Run(() => WatchDlq(nats, js, cts.Token), cts.Token);
		await Consume(js, cts.Token);
		break;
	default:
		Console.WriteLine("usage: bootstrap | publish | schedule | consume | dlq | all");
		break;
}

// Creates/updates the XTEL_PREPROCESSOR and XTEL_PROCESSOR ingestion streams,
// a shared XTEL_DLQ stream for max-delivery advisories, and a durable pull
// consumer on each ingestion stream.
//
// Both ingestion streams use:
//   - Retention = Workqueue      (message is removed when a consumer acks it)
//   - Discard   = New            (when stream limits are hit, reject new pubs
//                                 instead of dropping older messages)
//   - AllowMsgSchedules = true   (native 2.12 delayed delivery per-stream)
//   - AllowMsgTTL       = true   (required when schedule messages carry TTL)
//   - no MaxAge set              (messages never expire by time)
//
// Combined: nothing is ever silently dropped — a message stays in the stream
// until it is acked, and if capacity runs out the producer gets a rejected
// PubAck rather than having older messages discarded.
static async Task Bootstrap(INatsJSContext natsContext, CancellationToken ct)
{
	Log.Section("BOOTSTRAP");

	await CreateIngestionStream(natsContext, "XTEL_PREPROCESSOR", ct);
	await CreateIngestionStream(natsContext, "XTEL_PROCESSOR", ct);

	Log.Step("Creating/updating stream XTEL_DLQ (captures max-delivery advisories)");
	await natsContext.CreateOrUpdateStreamAsync(new StreamConfig("XTEL_DLQ",
	[
		"dlq.xtel.>",
		"$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.XTEL_PREPROCESSOR.*",
		"$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.XTEL_PROCESSOR.*",
	])
	{
		Storage = StreamConfigStorage.File,
		NumReplicas = 3,
		Retention = StreamConfigRetention.Limits,
		Discard = StreamConfigDiscard.New,
		Metadata = new Dictionary<string, string>
		{
			{ "Name", "XTEL_DLQ" },
			{ "Service", "INGESTION" },
		},
	}, ct);
	await PrintStream(natsContext, "XTEL_DLQ", ct);

	await CreateWorker(natsContext, "XTEL_PREPROCESSOR", "xtel-preprocessor", ct);
	await CreateWorker(natsContext, "XTEL_PROCESSOR",    "xtel-processor",    ct);

	Log.Ok("bootstrap complete — streams: XTEL_PREPROCESSOR, XTEL_PROCESSOR, XTEL_DLQ / consumers: xtel-preprocessor, xtel-processor");
}

static async Task CreateIngestionStream(INatsJSContext js, string name, CancellationToken ct)
{
	Log.Step($"Creating/updating stream {name} (Workqueue, Discard=New, schedules enabled, no expiry)");
	await js.CreateOrUpdateStreamAsync(new StreamConfig(name, [$"{name}.>"])
	{
		Storage = StreamConfigStorage.File,
		NumReplicas = 3,
		Retention = StreamConfigRetention.Workqueue,
		Discard = StreamConfigDiscard.New,
		AllowMsgSchedules = true,
		AllowMsgTTL = true,
		Metadata = new Dictionary<string, string>
		{
			{ "Name", name },
			{ "Service", "INGESTION" },
		},
	}, ct);
	await PrintStream(js, name, ct);
}

static async Task CreateWorker(INatsJSContext js, string stream, string durable, CancellationToken ct)
{
	Log.Step($"Creating/updating consumer {stream}/{durable}");
	await js.CreateOrUpdateConsumerAsync(stream, new ConsumerConfig(durable)
	{
		AckPolicy = ConsumerConfigAckPolicy.Explicit,
		DeliverPolicy = ConsumerConfigDeliverPolicy.All,
		ReplayPolicy = ConsumerConfigReplayPolicy.Instant,
		FilterSubject = $"{stream}.incoming",
		AckWait = TimeSpan.FromSeconds(30),
		MaxDeliver = 5,
		Backoff =
		[
			TimeSpan.FromSeconds(2),
			TimeSpan.FromSeconds(5),
			TimeSpan.FromSeconds(15),
			TimeSpan.FromSeconds(30),
			TimeSpan.FromSeconds(60),
		],
	}, ct);
	await PrintConsumer(js, stream, durable, ct);
}

// 1) Reliable publish with per-message dedup id + PubAck (no-loss on the producer side).
static async Task Publish(INatsJSContext js, CancellationToken ct)
{
	Log.Section("PUBLISH (5 messages to XTEL_PREPROCESSOR.incoming)");
	for (var i = 0; i < 5; i++)
	{
		var payload = JsonSerializer.SerializeToUtf8Bytes(new { id = Guid.NewGuid(), n = i });
		var msgId = $"xtel-preprocessor-{i}-{DateTime.UtcNow:yyyyMMddHHmmss}";
		var opts = new NatsJSPubOpts { MsgId = msgId, RetryAttempts = 3 };

		Log.Step($"publish #{i + 1} subject=XTEL_PREPROCESSOR.incoming msgId={msgId} bytes={payload.Length}");
		var ack = await js.PublishAsync("XTEL_PREPROCESSOR.incoming", payload, opts: opts, cancellationToken: ct);
		ack.EnsureSuccess();
		Log.Ok($"  ack stream={ack.Stream} seq={ack.Seq} duplicate={ack.Duplicate}");
	}
}

// 2) Native NATS 2.12 scheduled delivery via headers:
//      Nats-Schedule:        "@at <RFC3339>"
//      Nats-Schedule-Target: "<subject>"
//      Nats-Schedule-TTL:    "<duration>"
//    Publishing to XTEL_PREPROCESSOR.schedule.<id> stores the schedule in the
//    stream; at the fire time the body is emitted to XTEL_PREPROCESSOR.incoming
//    where the xtel-preprocessor consumer picks it up normally.
static async Task PublishScheduled(INatsJSContext js, CancellationToken ct)
{
	Log.Section("SCHEDULE (+30s delayed delivery to XTEL_PREPROCESSOR.incoming)");

	var fireAt = DateTimeOffset.UtcNow.AddSeconds(30);
	var rfc3339 = fireAt.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
	var scheduleSubject = $"XTEL_PREPROCESSOR.schedule.{Guid.NewGuid():N}";

	var headers = new NatsHeaders
	{
		["Nats-Schedule"]        = $"@at {rfc3339}",
		["Nats-Schedule-Target"] = "XTEL_PREPROCESSOR.incoming",
		["Nats-Schedule-TTL"]    = "5m",
	};
	foreach (var (k, v) in headers) Log.Kv($"header {k}", v.ToString());

	var body = JsonSerializer.SerializeToUtf8Bytes(new { msg = "delayed preprocessor work", fireAt });
	Log.Step($"publish {scheduleSubject} bytes={body.Length}");
	var ack = await js.PublishAsync(scheduleSubject, body, headers: headers, cancellationToken: ct);
	ack.EnsureSuccess();
	Log.Ok($"  ack stream={ack.Stream} seq={ack.Seq} fireAt={fireAt:O} (in ~{(fireAt - DateTimeOffset.UtcNow).TotalSeconds:n0}s)");
}

// 3) Resilient consumer. Explicit ack + bounded retries are defined on the
//    consumer, so we only need to Ack on success / Nak on transient failure.
static async Task Consume(INatsJSContext js, CancellationToken ct)
{
	Log.Section("CONSUME XTEL_PREPROCESSOR/xtel-preprocessor (40% simulated failure rate)");
	var consumer = await js.GetConsumerAsync("XTEL_PREPROCESSOR", "xtel-preprocessor", ct);
	Log.Kv("pending", consumer.Info.NumPending.ToString());
	Log.Kv("ack-pending", consumer.Info.NumAckPending.ToString());
	Log.Kv("delivered", consumer.Info.Delivered.StreamSeq.ToString());
	Log.Ok("starting consume loop…");
	await foreach (var msg in consumer.ConsumeAsync<byte[]>(cancellationToken: ct))
	{
		var seq = msg.Metadata?.Sequence.Stream ?? 0;
		var attempt = msg.Metadata?.NumDelivered ?? 0;
		var preview = msg.Data is null ? "" : System.Text.Encoding.UTF8.GetString(msg.Data.AsSpan(0, Math.Min(msg.Data.Length, 80)));
		Log.Step($"received subject={msg.Subject} seq={seq} attempt={attempt} body={preview}");

		try
		{
			if (Random.Shared.NextDouble() < 0.4)
				throw new InvalidOperationException("transient failure");

			await msg.AckAsync(cancellationToken: ct);
			Log.Ok($"  ack ok  subject={msg.Subject} seq={seq}");
		}
		catch (Exception ex)
		{
			await msg.NakAsync(cancellationToken: ct);
			var remaining = Math.Max(0, 5 - attempt);
			Log.Warn($"  nak     subject={msg.Subject} seq={seq} attempt={attempt}/5 retriesLeft={remaining} reason={ex.Message}");
		}
	}
}

// 4) DLQ watcher: on the 5th failure, the max-delivery advisory fires. We pull
//    the dead message by seq from the source stream and republish its body to
//    dlq.xtel.<stream>.<original-subject>. Advisory + payload both land in
//    XTEL_DLQ for inspection.
static async Task WatchDlq(NatsClient nats, INatsJSContext js, CancellationToken ct)
{
	const string advSubject = "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.*.*";
	Log.Kv("subscribing", advSubject);
	await foreach (var adv in nats.SubscribeAsync<byte[]>(advSubject, cancellationToken: ct))
	{
		if (adv.Data is null) continue;
		var json = JsonDocument.Parse(adv.Data);
		var stream = json.RootElement.GetProperty("stream").GetString()!;
		var seq = json.RootElement.GetProperty("stream_seq").GetUInt64();
		var consumer = json.RootElement.TryGetProperty("consumer", out var c) ? c.GetString() : "?";
		var deliveries = json.RootElement.TryGetProperty("deliveries", out var d) ? d.GetInt32() : 0;

		Log.Err($"DLQ advisory  stream={stream} consumer={consumer} seq={seq} deliveries={deliveries}");

		var jsStream = await js.GetStreamAsync(stream, cancellationToken: ct);
		var raw = await jsStream.GetAsync(new StreamMsgGetRequest { Seq = seq }, ct);
		var dlqSubject = $"dlq.xtel.{stream}.{raw.Message.Subject}";
		var ack = await js.PublishAsync(dlqSubject, raw.Message.Data, cancellationToken: ct);
		Log.Ok($"  republished to {dlqSubject} (dlq stream seq={ack.Seq}, bytes={raw.Message.Data.Length})");
	}
}

static async Task PrintStream(INatsJSContext js, string name, CancellationToken ct)
{
	var s = await js.GetStreamAsync(name, cancellationToken: ct);
	var cfg = s.Info.Config;
	var state = s.Info.State;
	Log.Ok($"[OK] stream {name}");
	Log.Kv("  subjects", string.Join(",", cfg.Subjects ?? []));
	Log.Kv("  storage", cfg.Storage.ToString());
	Log.Kv("  replicas", cfg.NumReplicas.ToString());
	Log.Kv("  retention", cfg.Retention.ToString());
	Log.Kv("  discard", cfg.Discard.ToString());
	Log.Kv("  max-age", cfg.MaxAge == TimeSpan.Zero ? "(never — no expiry)" : cfg.MaxAge.ToString());
	Log.Kv("  max-bytes", cfg.MaxBytes < 0 ? "(unlimited)" : cfg.MaxBytes.ToString());
	Log.Kv("  dup-window", cfg.DuplicateWindow.ToString());
	Log.Kv("  allow-msg-ttl", cfg.AllowMsgTTL.ToString());
	Log.Kv("  allow-schedules", cfg.AllowMsgSchedules.ToString());
	Log.Kv("  messages", state.Messages.ToString());
	Log.Kv("  bytes", state.Bytes.ToString());
	Log.Kv("  first-seq", state.FirstSeq.ToString());
	Log.Kv("  last-seq", state.LastSeq.ToString());
	Log.Kv("  consumers", state.ConsumerCount.ToString());
}

static async Task PrintConsumer(INatsJSContext js, string stream, string name, CancellationToken ct)
{
	var c = await js.GetConsumerAsync(stream, name, ct);
	var cfg = c.Info.Config;
	Log.Ok($"[OK] consumer {stream}/{name}");
	Log.Kv("  durable", cfg.DurableName ?? "");
	Log.Kv("  filter-subject", cfg.FilterSubject ?? "");
	Log.Kv("  ack-policy", cfg.AckPolicy.ToString());
	Log.Kv("  deliver-policy", cfg.DeliverPolicy.ToString());
	Log.Kv("  replay-policy", cfg.ReplayPolicy.ToString());
	Log.Kv("  ack-wait", cfg.AckWait.ToString());
	Log.Kv("  max-deliver", cfg.MaxDeliver.ToString());
	Log.Kv("  backoff", cfg.Backoff is null ? "(none)" : string.Join(",", cfg.Backoff));
	Log.Kv("  pending", c.Info.NumPending.ToString());
	Log.Kv("  ack-pending", c.Info.NumAckPending.ToString());
	Log.Kv("  redelivered", c.Info.NumRedelivered.ToString());
}

static class Log
{
	static readonly object Gate = new();
	static string Ts() => DateTime.Now.ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture);

	public static void Banner(string title)
	{
		lock (Gate)
		{
			var line = new string('=', 72);
			Console.ForegroundColor = ConsoleColor.Cyan;
			Console.WriteLine(line);
			Console.WriteLine($"  {title}");
			Console.WriteLine(line);
			Console.ResetColor();
		}
	}

	public static void Section(string title)
	{
		lock (Gate)
		{
			Console.ForegroundColor = ConsoleColor.Cyan;
			Console.WriteLine();
			Console.WriteLine($"--- {title} " + new string('-', Math.Max(1, 70 - title.Length)));
			Console.ResetColor();
		}
	}

	public static void Step(string msg) => Write(ConsoleColor.White, "-> ", msg);
	public static void Ok(string msg) => Write(ConsoleColor.Green, "[OK] ", msg);
	public static void Warn(string msg) => Write(ConsoleColor.Yellow, "[!!] ", msg);
	public static void Err(string msg) => Write(ConsoleColor.Red, "[ERR] ", msg);
	public static void Kv(string k, string v) => Write(ConsoleColor.Gray, "     ", $"{k,-18} = {v}");

	static void Write(ConsoleColor color, string prefix, string msg)
	{
		lock (Gate)
		{
			Console.ForegroundColor = ConsoleColor.DarkGray;
			Console.Write($"{Ts()} ");
			Console.ForegroundColor = color;
			Console.Write(prefix);
			Console.WriteLine(msg);
			Console.ResetColor();
		}
	}
}
