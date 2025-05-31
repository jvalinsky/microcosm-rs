use clap::Parser;
use jetstream::events::Cursor;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};
use ufos::consumer;
use ufos::file_consumer;
use ufos::server;
use ufos::storage::{StorageWhatever, StoreBackground, StoreReader, StoreWriter};
use ufos::storage_fjall::FjallStorage;
use ufos::storage_mem::MemStorage;
use ufos::store_types::SketchSecretPrefix;
use ufos::{nice_duration, ConsumerInfo};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

/// Aggregate links in the at-mosphere
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Jetstream server to connect to (exclusive with --fixture). Provide either a wss:// URL, or a shorhand value:
    /// 'us-east-1', 'us-east-2', 'us-west-1', or 'us-west-2'
    #[arg(long)]
    jetstream: String,
    /// allow changing jetstream endpoints
    #[arg(long, action)]
    jetstream_force: bool,
    /// don't request zstd-compressed jetstream events
    ///
    /// reduces CPU at the expense of more ingress bandwidth
    #[arg(long, action)]
    jetstream_no_zstd: bool,
    /// Location to store persist data to disk
    #[arg(long)]
    data: PathBuf,
    /// DEBUG: don't start the jetstream consumer or its write loop
    #[arg(long, action)]
    pause_writer: bool,
    /// Adjust runtime settings like background task intervals for efficient backfill
    #[arg(long, action)]
    backfill: bool,
    /// DEBUG: force the rw loop to fall behind  by pausing it
    /// todo: restore this
    #[arg(long, action)]
    pause_rw: bool,
    /// DEBUG: use an in-memory store instead of fjall
    #[arg(long, action)]
    in_mem: bool,
    /// reset the rollup cursor, scrape through missed things in the past (backfill)
    #[arg(long, action)]
    reroll: bool,
    /// DEBUG: interpret jetstream as a file fixture
    #[arg(long, action)]
    jetstream_fixture: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::parse();
    let jetstream = args.jetstream.clone();
    if args.in_mem {
        let (read_store, write_store, cursor, sketch_secret) = MemStorage::init(
            args.data,
            jetstream,
            args.jetstream_force,
            Default::default(),
        )?;
        go(
            args.jetstream,
            args.jetstream_fixture,
            args.pause_writer,
            args.backfill,
            args.reroll,
            read_store,
            write_store,
            cursor,
            sketch_secret,
        )
        .await?;
    } else {
        let (read_store, write_store, cursor, sketch_secret) = FjallStorage::init(
            args.data,
            jetstream,
            args.jetstream_force,
            Default::default(),
        )?;
        go(
            args.jetstream,
            args.jetstream_fixture,
            args.pause_writer,
            args.backfill,
            args.reroll,
            read_store,
            write_store,
            cursor,
            sketch_secret,
        )
        .await?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn go<B: StoreBackground>(
    jetstream: String,
    jetstream_fixture: bool,
    pause_writer: bool,
    backfill: bool,
    reroll: bool,
    read_store: impl StoreReader + 'static + Clone,
    mut write_store: impl StoreWriter<B> + 'static,
    cursor: Option<Cursor>,
    sketch_secret: SketchSecretPrefix,
) -> anyhow::Result<()> {
    println!("starting server with storage...");
    let serving = server::serve(read_store.clone());

    if pause_writer {
        log::info!("not starting jetstream or the write loop.");
        serving.await.map_err(|e| anyhow::anyhow!(e))?;
        return Ok(());
    }

    let batches = if jetstream_fixture {
        log::info!("starting with jestream file fixture: {jetstream:?}");
        file_consumer::consume(jetstream.into(), sketch_secret, cursor).await?
    } else {
        log::info!(
            "starting consumer with cursor: {cursor:?} from {:?} ago",
            cursor.map(|c| c.elapsed())
        );
        consumer::consume(&jetstream, cursor, false, sketch_secret).await?
    };

    let rolling = write_store.background_tasks(reroll)?.run(backfill);
    let storing = write_store.receive_batches(batches);

    let stating = do_update_stuff(read_store);

    tokio::select! {
        z = serving => log::warn!("serve task ended: {z:?}"),
        z = rolling => log::warn!("rollup task ended: {z:?}"),
        z = storing => log::warn!("storage task ended: {z:?}"),
        z = stating => log::warn!("status task ended: {z:?}"),
    };

    println!("bye!");

    Ok(())
}

async fn do_update_stuff(read_store: impl StoreReader) {
    let started_at = std::time::SystemTime::now();
    let mut first_cursor = None;
    let mut first_rollup = None;
    let mut last_at = std::time::SystemTime::now();
    let mut last_cursor = None;
    let mut last_rollup = None;
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(4));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        match read_store.get_consumer_info().await {
            Err(e) => log::warn!("failed to get jetstream consumer info: {e:?}"),
            Ok(ConsumerInfo::Jetstream {
                latest_cursor,
                rollup_cursor,
                ..
            }) => {
                let now = std::time::SystemTime::now();
                let latest_cursor = latest_cursor.map(Cursor::from_raw_u64);
                let rollup_cursor = rollup_cursor.map(Cursor::from_raw_u64);
                backfill_info(
                    latest_cursor,
                    rollup_cursor,
                    last_cursor,
                    last_rollup,
                    last_at,
                    first_cursor,
                    first_rollup,
                    started_at,
                    now,
                );
                first_cursor = first_cursor.or(latest_cursor);
                first_rollup = first_rollup.or(rollup_cursor);
                last_cursor = latest_cursor;
                last_rollup = rollup_cursor;
                last_at = now;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn backfill_info(
    latest_cursor: Option<Cursor>,
    rollup_cursor: Option<Cursor>,
    last_cursor: Option<Cursor>,
    last_rollup: Option<Cursor>,
    last_at: SystemTime,
    first_cursor: Option<Cursor>,
    first_rollup: Option<Cursor>,
    started_at: SystemTime,
    now: SystemTime,
) {
    let nice_dt_two_maybes = |earlier: Option<Cursor>, later: Option<Cursor>| match (earlier, later)
    {
        (Some(earlier), Some(later)) => match later.duration_since(&earlier) {
            Ok(dt) => nice_duration(dt),
            Err(e) => {
                let rev_dt = e.duration();
                format!("+{}", nice_duration(rev_dt))
            }
        },
        _ => "unknown".to_string(),
    };

    let rate = |mlatest: Option<Cursor>, msince: Option<Cursor>, real: Duration| {
        mlatest
            .zip(msince)
            .map(|(latest, since)| {
                latest
                    .duration_since(&since)
                    .unwrap_or(Duration::from_millis(1))
            })
            .map(|dtc| format!("{:.2}", dtc.as_secs_f64() / real.as_secs_f64()))
            .unwrap_or("??".into())
    };

    let dt_real = now
        .duration_since(last_at)
        .unwrap_or(Duration::from_millis(1));

    let dt_real_total = now
        .duration_since(started_at)
        .unwrap_or(Duration::from_millis(1));

    let cursor_rate = rate(latest_cursor, last_cursor, dt_real);
    let cursor_avg = rate(latest_cursor, first_cursor, dt_real_total);

    let rollup_rate = rate(rollup_cursor, last_rollup, dt_real);
    let rollup_avg = rate(rollup_cursor, first_rollup, dt_real_total);

    log::info!(
        "cursor: {} behind (→{}, {cursor_rate}x, {cursor_avg}x avg). rollup: {} behind (→{}, {rollup_rate}x, {rollup_avg}x avg).",
        latest_cursor.map(|c| c.elapsed().map(nice_duration).unwrap_or("++".to_string())).unwrap_or("?".to_string()),
        nice_dt_two_maybes(last_cursor, latest_cursor),
        rollup_cursor.map(|c| c.elapsed().map(nice_duration).unwrap_or("++".to_string())).unwrap_or("?".to_string()),
        nice_dt_two_maybes(last_rollup, rollup_cursor),
    );
}
