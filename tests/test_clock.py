import time
from datetime import datetime, timezone

from generator.clock import RealClock, VirtualClock, iso


def check_virtual_clock_does_not_sleep_when_speedup_is_huge():
    c = VirtualClock(speedup=1e9)
    t0 = time.monotonic()
    for n in range(200):
        c.sleep_until_sim(n * 0.5)
    assert time.monotonic() - t0 < 0.2, "virtual clock slept when it should not have"
    assert abs(c.sim_elapsed() - 99.5) < 1e-9, c.sim_elapsed()


def check_virtual_clock_advances_simulated_time_without_wall_time():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    c = VirtualClock(start=start, speedup=1e9)
    c.sleep_until_sim(3600.0)
    assert c.now() == datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc), c.now()


def check_virtual_clock_rejects_a_speedup_of_zero():
    # Zero would divide by zero inside the pacer rather than fail here, which is a
    # much worse place to find out.
    for bad in (0.0, -1.0):
        try:
            VirtualClock(speedup=bad)
        except ValueError as e:
            assert "speedup" in str(e), str(e)
        else:
            raise AssertionError(f"speedup {bad} was accepted")


def check_simulated_time_never_goes_backwards():
    c = VirtualClock(speedup=1e9)
    c.sleep_until_sim(10.0)
    c.sleep_until_sim(4.0)
    assert c.sim_elapsed() == 10.0, c.sim_elapsed()


def check_real_clock_sleeps_and_reports_what_it_slept():
    c = RealClock()
    slept = c.sleep_until_sim(0.05)
    assert slept > 0, slept
    assert c.sim_elapsed() >= 0.05, c.sim_elapsed()
    # Already past, so nothing to wait for.
    assert c.sleep_until_sim(0.01) == 0.0


def check_iso_is_zulu_with_milliseconds():
    ts = datetime(2026, 8, 14, 16, 5, 1, 482000, tzinfo=timezone.utc)
    assert iso(ts) == "2026-08-14T16:05:01.482Z", iso(ts)
