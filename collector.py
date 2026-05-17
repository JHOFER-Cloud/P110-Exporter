import asyncio
import os
from contextlib import contextmanager
from datetime import datetime
from enum import Enum, auto
from math import floor
from time import time, sleep

from loguru import logger
from prometheus_client import Histogram
from prometheus_client.core import GaugeMetricFamily
from tapo import ApiClient
from tapo.requests import EnergyDataInterval


OBSERVATION_RED_METRICS = Histogram(
    "tapo_p110_observation_rate_ms",
    "RED metrics for queries to the TP-Link TAPO P110 devices. (milliseconds)",
    labelnames=["ip_address", "room", "success"],
    buckets=(10, 100, 150, 200, 250, 300, 500, 750, 1000, 1500, 2000)
)


class MetricType(Enum):
    DEVICE_COUNT = auto()
    TODAY_RUNTIME = auto()
    MONTH_RUNTIME = auto()
    TODAY_ENERGY = auto()
    MONTH_ENERGY = auto()
    CURRENT_POWER = auto()
    PREVIOUS_MONTH_ENERGY = auto()


def get_metrics():
    return {
        MetricType.DEVICE_COUNT: GaugeMetricFamily(
            "tapo_p110_device_count",
            "Number of available TP-Link TAPO P110 Smart Sockets.",
        ),
        MetricType.TODAY_RUNTIME: GaugeMetricFamily(
            "tapo_p110_today_runtime_mins",
            "Current running time for the TP-Link TAPO P110 Smart Socket today. (minutes)",
            labels=["ip_address", "room"],
        ),
        MetricType.MONTH_RUNTIME: GaugeMetricFamily(
            "tapo_p110_month_runtime_mins",
            "Current running time for the TP-Link TAPO P110 Smart Socket this month. (minutes)",
            labels=["ip_address", "room"],
        ),
        MetricType.TODAY_ENERGY: GaugeMetricFamily(
            "tapo_p110_today_energy_wh",
            "Energy consumed by the TP-Link TAPO P110 Smart Socket today. (Watt-hours)",
            labels=["ip_address", "room"],
        ),
        MetricType.MONTH_ENERGY: GaugeMetricFamily(
            "tapo_p110_month_energy_wh",
            "Energy consumed by the TP-Link TAPO P110 Smart Socket this month. (Watt-hours)",
            labels=["ip_address", "room"],
        ),
        MetricType.CURRENT_POWER: GaugeMetricFamily(
            "tapo_p110_power_consumption_w",
            "Current power consumption for TP-Link TAPO P110 Smart Socket. (milliwatts; divide by 1000 for watts)",
            labels=["ip_address", "room"],
        ),
        MetricType.PREVIOUS_MONTH_ENERGY: GaugeMetricFamily(
            "tapo_p110_previous_month_energy_wh",
            "Energy consumed by the TP-Link TAPO P110 Smart Socket during the previous calendar month. (Watt-hours)",
            labels=["ip_address", "room"],
        ),
    }


RED_SUCCESS = "SUCCESS"
RED_FAILURE = "FAILURE"


@contextmanager
def time_observation(ip_address, room):
    caught = None
    status = RED_SUCCESS
    start = time()

    try:
        yield
    except Exception as e:
        status = RED_FAILURE
        caught = e

    duration = floor((time() - start) * 1000)
    OBSERVATION_RED_METRICS.labels(ip_address=ip_address, room=room, success=status).observe(duration)

    logger.debug("observation completed", extra={
        "ip": ip_address, "room": room, "duration_ms": duration,
    })

    if caught:
        raise caught


class Collector:
    def __init__(self, deviceMap, email_address, password):
        self.email_address = email_address
        self.password = password
        self.loop = asyncio.new_event_loop()
        self.client = ApiClient(email_address, password)

        self.devices = {
            room: (ip_address, device)
            for room, ip_address in deviceMap.items()
            if (device := self._connect(ip_address, room)) is not None
        }

    def _connect(self, ip_address, room):
        extra = {"ip": ip_address, "room": room}
        logger.debug("connecting to device", extra=extra)

        max_retries = int(os.getenv("MAX_RETRY_COUNT", 3))
        attempts = 0

        while True:
            try:
                device = self.loop.run_until_complete(self.client.p110(ip_address))
                logger.debug("successfully connected to device", extra=extra)
                return device
            except Exception:
                attempts += 1
                logger.error("failed to connect to device", extra=extra, exc_info=True)
                if max_retries != 0 and attempts >= max_retries:
                    return None
                sleep(1)

    def get_device_data(self, device, ip_address, room):
        with time_observation(ip_address, room):
            logger.debug("retrieving energy usage statistics for device", extra={
                "ip": ip_address, "room": room,
            })
            try:
                return self.loop.run_until_complete(device.get_energy_usage())
            except Exception as e:
                logger.warning("failed to retrieve energy usage, resetting connection", extra={
                    "ip": ip_address, "room": room, "error": str(e),
                })
                new_device = self._connect(ip_address, room)
                if new_device is None:
                    raise
                self.devices[room] = (ip_address, new_device)
                return self.loop.run_until_complete(new_device.get_energy_usage())

    def get_previous_month_energy(self, device, ip_address, room):
        """Fetch last calendar month's total energy via the device's monthly history.

        Returns Wh, or None if unavailable. The device's `get_energy_data` Monthly
        call requires a `start_date` aligned to Jan 1 of some year, and returns up
        to 12 entries from that year (current/running month not included). We work
        out which (year, month) we want, query that year, and match by
        `start_date_time` prefix rather than by position.
        """
        now = datetime.now()
        if now.month == 1:
            target_year, target_month = now.year - 1, 12
        else:
            target_year, target_month = now.year, now.month - 1

        try:
            start = datetime(target_year, 1, 1)

            async def fetch():
                return await device.get_energy_data(EnergyDataInterval.Monthly, start)

            result = self.loop.run_until_complete(fetch())
        except Exception:
            logger.exception("failed to fetch monthly history", extra={
                "ip": ip_address, "room": room,
            })
            return None

        try:
            data = result.to_dict() if hasattr(result, "to_dict") else None
            entries = data.get("entries", []) if isinstance(data, dict) else getattr(result, "entries", [])
        except Exception:
            logger.exception("could not read entries from energy data response")
            return None

        target_prefix = f"{target_year:04d}-{target_month:02d}"
        for entry in entries:
            sdt = entry.get("start_date_time") if isinstance(entry, dict) else getattr(entry, "start_date_time", None)
            if str(sdt).startswith(target_prefix):
                value = entry.get("energy") if isinstance(entry, dict) else getattr(entry, "energy", None)
                if isinstance(value, (int, float)):
                    return value

        logger.warning("could not find entry for target month in monthly history", extra={
            "ip": ip_address, "room": room, "target": target_prefix,
            "available": [
                (entry.get("start_date_time") if isinstance(entry, dict) else getattr(entry, "start_date_time", None))
                for entry in entries
            ],
        })
        return None

    def collect(self):
        logger.info("receiving prometheus metrics scrape: collecting observations")

        metrics = get_metrics()
        metrics[MetricType.DEVICE_COUNT].add_metric([], len(self.devices))

        for room, (ip_addr, device) in self.devices.items():
            logger.info("performing observations for device", extra={
                "ip": ip_addr, "room": room,
            })

            try:
                data = self.get_device_data(device, ip_addr, room)
                labels = [ip_addr, room]
                metrics[MetricType.TODAY_RUNTIME].add_metric(labels, data.today_runtime)
                metrics[MetricType.MONTH_RUNTIME].add_metric(labels, data.month_runtime)
                metrics[MetricType.TODAY_ENERGY].add_metric(labels, data.today_energy)
                metrics[MetricType.MONTH_ENERGY].add_metric(labels, data.month_energy)
                metrics[MetricType.CURRENT_POWER].add_metric(labels, data.current_power)

                prev_month = self.get_previous_month_energy(device, ip_addr, room)
                if prev_month is not None:
                    metrics[MetricType.PREVIOUS_MONTH_ENERGY].add_metric(labels, prev_month)
            except Exception:
                logger.exception("encountered exception during observation!")

        for m in metrics.values():
            yield m
