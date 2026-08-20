"""
launch_producer.py
--------------------------------------------------------------------------
Imports the synthetic event generator (eventProducer.py) and launches it
as a separate background process that continuously streams events to Kafka.

Why a separate process (not just calling the function inline):
  - The generator's Kafka send loop is blocking/CPU+IO bound; running it in
    its own process means it doesn't block whatever else you're doing in the
    parent (e.g. launching a Spark consumer, running other setup, or an
    interactive session).
  - multiprocessing.Process gives you a real OS process you can independently
    terminate/monitor, and each process gets its own numpy Generator seeded
    deterministically (seed + worker offset) so they don't produce identical
    output if you ever scale to multiple producer workers.
  - KafkaProducer objects hold live socket connections; those should not be
    shared across a fork boundary, so producing in a dedicated process
    (which creates its own KafkaProducer inside eventProducer.py) avoids
    that whole class of bugs.

Since eventProducer.generate_events_to_csv() as written produces a FIXED
number of events and then returns, this launcher wraps it in a loop so it
behaves like a continuous stream -- each iteration generates a fresh batch
and sends it to Kafka, then (optionally) sleeps before the next batch.

Usage:
    python launch_producer.py \
        --n-users 1000 --n-products 200 \
        --batch-size 500 --interval-seconds 5 \
        --kafka-broker localhost:9092 \
        --out-dir ./data --seed 42

    # run indefinitely (Ctrl+C to stop)
    python launch_producer.py --run-forever

    # run a fixed number of batches then stop
    python launch_producer.py --max-batches 20
--------------------------------------------------------------------------
"""

import argparse
import multiprocessing as mp
import os
import signal
import sys
import time

# Import the generator module -- expects eventProducer.py in the same
# directory (or on PYTHONPATH).
import eventProducer


def producer_worker(args: argparse.Namespace) -> None:
    """Runs inside the child process. Repeatedly calls the generator's
    Kafka-streaming function in batches, forever or up to --max-batches."""
    rng = eventProducer.np.random.default_rng(args.seed)

    events_path = os.path.join(args.out_dir, args.file_name)
    os.makedirs(args.out_dir, exist_ok=True)

    batch_num = 0
    print(f"[producer pid={os.getpid()}] starting -- broker={args.kafka_broker} "
          f"batch_size={args.batch_size} interval={args.interval_seconds}s")

    try:
        while args.max_batches is None or batch_num < args.max_batches:
            batch_num += 1
            print(f"[producer pid={os.getpid()}] batch {batch_num} "
                  f"({'unbounded' if args.max_batches is None else args.max_batches}) starting...")

            eventProducer.generate_events_to_csv(
                path=events_path,
                n_events=args.batch_size,
                n_users=args.n_users,
                n_products=args.n_products,
                chunk_size=args.batch_size,   # one chunk per batch, keeps memory bounded
                rng=rng,
                stream=True,                  # always push to Kafka in the producer worker
                broker=args.kafka_broker,
            )

            if args.max_batches is None or batch_num < args.max_batches:
                time.sleep(args.interval_seconds)

    except KeyboardInterrupt:
        pass
    finally:
        print(f"[producer pid={os.getpid()}] stopped after {batch_num} batch(es)")


def parse_args():
    parser = argparse.ArgumentParser(description="Launch the event generator as a background Kafka producer process")
    parser.add_argument("--n-users", type=int, default=1000)
    parser.add_argument("--n-products", type=int, default=200)
    parser.add_argument("--batch-size", type=int, default=500,
                         help="Number of events generated and sent per batch")
    parser.add_argument("--interval-seconds", type=float, default=5.0,
                         help="Pause between batches, to simulate a steady trickle instead of a burst")
    parser.add_argument("--max-batches", type=int, default=None,
                         help="Stop after this many batches. Omit (or use --run-forever) to run indefinitely")
    parser.add_argument("--run-forever", action="store_true",
                         help="Explicit flag for running with no batch limit (equivalent to omitting --max-batches)")
    parser.add_argument("--kafka-broker", type=str, default="localhost:9092")
    parser.add_argument("--out-dir", type=str, default=".")
    parser.add_argument("--file-name", type=str, default="events_from_kafka.csv",
                        help="CSV filename to write inside --out-dir. Compatibility copies are also created as events.csv.")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    if args.run_forever:
        args.max_batches = None
    return args


def main():
    args = parse_args()

    ctx = mp.get_context("spawn")  # spawn avoids inheriting live sockets/threads from the parent
    process = ctx.Process(target=producer_worker, args=(args,), name="kafka-event-producer", daemon=False)
    process.start()
    print(f"[launcher] started producer process pid={process.pid}")

    def handle_signal(signum, frame):
        print(f"\n[launcher] received signal {signum}, terminating producer pid={process.pid}...")
        process.terminate()
        process.join(timeout=10)
        if process.is_alive():
            print("[launcher] producer did not exit in time, killing it")
            process.kill()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Block here so the launcher stays alive to supervise the child;
    # Ctrl+C triggers handle_signal above via SIGINT.
    process.join()
    print(f"[launcher] producer process exited with code {process.exitcode}")


if __name__ == "__main__":
    main()
