#!/usr/bin/env python3

import gpsmon
import gpio
import reporter
import vedirect

import argparse
import configparser
import contextlib
import datetime
import enum
import json
import logging
import os
import pathlib
import subprocess
import sys
import threading
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]

@enum.unique
class Events(enum.IntEnum):
    POWER_ON                  = 1
    SERVICE_EXITED            = 2
    PUMP_ON_SCHEDULE          = 3
    PUMP_ON_FORCE             = 4
    PUMP_OFF_SCHEDULE         = 5
    PUMP_OFF_LOAD_CURRENT     = 6
    PUMP_OFF_SOC_LOW          = 7
    PUMP_OFF_EXITED           = 8
    PUMP_OFF_FORCE            = 9
    GPS_SPEED_EXCEEDED        = 10
    GPS_GOT_FIX               = 11
    BATT_FULL_CHARGE          = 12
    SETTINGS_CHANGE           = 13
    MPPT_ABSORPTION_STATE     = 14
    MPPT_ERROR                = 15
    INPUT_CHANGED             = 16

class MPPTChargeState(enum.IntEnum):
    OFF          = 0
    LOW_POWER    = 1
    FAULT        = 2
    BULK         = 3
    ABSORPTION   = 4
    FLOAT        = 5
    STORAGE      = 6
    EQUALIZE     = 7
    INVERTING    = 9
    POWER_SUPPLY = 11

class InvalidSetting(Exception):
    pass

class Settings:
    InvalidSetting = InvalidSetting

    _log = logging.getLogger("settings")
    _json_file = ROOT / "settings.json"

    _valid_settings = ("min_soc", "turn_on_soc", "min_load", "max_load",
                       "load_delay", "load_detect", "load_min_voltage",
                       "reset_load_error", "report_interval",
                       "sched_period", "sched_intervals", "force_pump_on",
                       "force_pump_off", "gps_speed_thresh", "sched_mon",
                       "sched_Tue", "sched_wed", "sched_thu", "sched_fri",
                       "sched_sat", "sched_sun", "schedule_start", "schedule_stop",
                       "timezone", "restart_mppt")

    # Hidden Settings can only be set with a local config file,
    # not from server commands
    _hidden_settings = ("broker", "broker_use_tls", "broker_ca_certs",
                        "broker_certfile", "broker_keyfile", "gpsd_server",
                        "batt_cap", "batt_type", "abs_thresh", "relay",
                        "inp_tank_too_low", "inp_leak_detect",
                        "inp_tank_refill")

    _default_settings = {
        # Turn off the pump when the state of charge is below this value
        "min_soc": 300, #(% x10)

        # Turn the pump on with the SOC is greater than this value
        "turn_on_soc": 900, #(% x10)

        # Alert when the pump is on and the load current is not between
        # these values
        "min_load": 6000,    #mA
        "max_load": 13000,   #mA

        # Reset the error flag to retry the pump
        "reset_load_error": False,

        # Delay in seconds after turning the pump on before checking the load current
        "load_delay": 60, #s

        # Delay for the load current to be out of range before hitting an alarm
        "load_detect": 120, #s

        # Ignore load errors if the battery voltage is below this threshold
        "load_min_voltage": 48000, #mV

        #Server hostname to report to
        "broker": "thingsboard.cloud",
        "broker_use_tls": True,
        "broker_ca_certs": "/boot/ca.crt",
        "broker_certfile": "/boot/client.crt",
        "broker_keyfile": "/boot/client.key",

        # GPSD Server
        "gpsd_server": "192.168.168.1",
        "gps_speed_thresh": 1.0,

        # Report Interval
        "report_interval": 30 * 60, #s

        # Schedule period can be hourly, daily or weekly
        "sched_period": "daily",

        # Schedule intervals when the pump should be turned on.
        # Should be a list of pairs of start and end times each in seconds.
        # Repeats every 'sched_period'
        "sched_intervals": [],

        # When sched_period is "daily", only turn on on days set
        # to True
        "sched_mon": True,
        "sched_Tue": True,  #sic
        "sched_wed": True,
        "sched_thu": True,
        "sched_fri": True,
        "sched_sat": True,
        "sched_sun": True,

        # If schedule_intervals is unset, use these values as the start and
        # stop time (this is easier for thingsboard)
        "schedule_start": 28800000, # in ms (8am)
        "schedule_stop": 57600000, # in ms (4pm)

        # When true, the pump will always be on
        "force_pump_on": False,

        # When true, the pump will never be turned on
        "force_pump_off": False,

        # Battery Capacity
        "batt_cap": None,
        "batt_type": "lifepo4",
        "abs_thresh": None,

        # Time Zone to calculate local time in
        "timezone": "America/Edmonton",

        #Relay to use
        "relay": 1,

        #Inputs
        "inp_tank_too_low": 1,
        "inp_leak_detect":  2,
        "inp_tank_refill":  3,

        #Force Restart the MPPT
        "restart_mppt": False,
    }

    def __init__(self):
        try:
            self._settings = None
            with self._json_file.open() as f:
                init_settings = json.load(f)
                self.validate(init_settings)
                self._settings = init_settings
        except InvalidSetting as e:
            self._log.error(f"Invalid settings file {self._json_file}: {e}")
        except FileNotFoundError:
            self._log.info(f"File not found: {self._json_file}")

        if self._settings is None:
            self._log.info("Using default settings")
            self._settings = self._default_settings.copy()

        for k, v in self._default_settings.items():
            if k not in self._settings:
                self._log.info(f"Setting default unset setting: {k}")
                self._settings[k] = v

        self.updateTimezone(self.timezone)
        self._update_handlers = {}

    def range_validator(min_val, max_val):
        def validate(self, name, value):
            try:
                if (min_val is not None) and value < min_val:
                    raise InvalidSetting(f"{name} must be greater than {min_val}")
                if (max_val is not None) and value > max_val:
                    raise InvalidSetting(f"{name} must be less than {max_val}")
            except TypeError:
                raise InvalidSetting(f"{name} must be numeric")
            return value
        return validate

    def bool_validator(self, name, value):
        if value is not True and value is not False:
            raise InvalidSetting(f"{name} must be true or false")
        return value

    validate_min_soc = range_validator(0, 1000)
    validate_turn_on_soc = range_validator(0, 1000)
    validate_min_load = range_validator(0, 50000)
    validate_max_load = range_validator(0, 50000)
    validate_load_delay = range_validator(0, None)
    validate_load_detect = range_validator(0, None)
    validate_load_min_voltage = range_validator(0, None)
    validate_reset_load_error = bool_validator
    validate_report_interval = range_validator(30, None)
    validate_gps_speed_thresh = range_validator(0.8, None)
    validate_force_pump_on = bool_validator
    validate_force_pump_off = bool_validator
    validate_sched_mon = bool_validator
    validate_sched_Tue = bool_validator
    validate_sched_wed = bool_validator
    validate_sched_thu = bool_validator
    validate_sched_fri = bool_validator
    validate_sched_sat = bool_validator
    validate_sched_sun = bool_validator
    validate_schedule_start = range_validator(0, 7 * 24 * 3600* 1000)
    validate_schedule_end = range_validator(0, 7 * 24 * 3600* 1000)
    validate_relay = range_validator(1, 4)
    validate_restart_mppt = bool_validator

    def validate_sched_period(self, name, value):
        value = value.lower()
        if value not in ("hourly", "daily", "weekly"):
            raise InvalidSetting(f"{name} must be 'hourly', 'daily' or 'weekly'")
        return value

    def validate_sched_intervals(self, name, value):
        exception = InvalidSetting(f"{name} must be a list of start and end times")

        try:
            for v in value:
                if len(v) != 2:
                    raise exception
                if v[0] < 0 or v[1] < 0 or v[1] < v[0]:
                    raise exception
        except TypeError:
            raise exception
        return value

    def validate_timezone(self, name, value):
        zoneinfo = pathlib.Path("/usr/share/zoneinfo")

        if not (zoneinfo / value).is_file():
            raise InvalidSetting(f"{name} is not a valid timezone: {value}")

        return value

    def validate_batt_type(self, name, value):
        return value.upper()

    def validate(self, settings):
        for k, v in settings.items():
            try:
                if k not in self._valid_settings and k not in self._hidden_settings:
                    raise InvalidSetting(f"Unknown setting: '{k}'")

                validator = getattr(self, f"validate_{k}", None)
                if validator is not None:
                    settings[k] = validator(k, v)
            except InvalidSetting as e:
                e.key = k
                raise

    def write(self):
        new_file = self._json_file.with_suffix(".json.new")
        with new_file.open("w") as f:
            json.dump(self._settings, f, indent=2)
        new_file.rename(self._json_file)

    def updateTimezone(self, value):
        if os.environ.get("TZ", None) == value:
            return

        self._log.info(f"Changed timezone to {value}")
        os.environ["TZ"] = value
        time.tzset()

    def update(self, new_settings):
        self.validate(new_settings)
        self._settings.update(new_settings)
        self._log.info("Updated Settings")

        if "timezone" in new_settings:
            self.updateTimezone(new_settings["timezone"])

        for func in self._update_handlers.values():
            func(new_settings)

    def updateFromFile(self, f):
        new_settings = json.load(f)
        self._log.info(f"Loaded settings from {f.name}")
        self.update(new_settings)

    def __getattr__(self, k):
        if self._settings is None:
            raise AttributeError(k)
        try:
            return self._settings[k]
        except KeyError:
            raise AttributeError(k)

    def registerUpdateHandler(self, name, func):
        self._update_handlers[name] = func

    def unregisterUpdateHandler(self, name):
        try:
            return self._update_handlers.pop(name)
        except KeyError:
            return None

    def keys(self):
        return self._valid_settings

    def getAbsThresh(self):
        if self.abs_thresh is not None:
            return self.abs_thresh

        if self.batt_type == "RBT50LFP48S-G2":
            return 53800

        return 55000

    def getBattCap(self):
        if self.batt_cap is not None:
            return self.batt_cap

        if self.batt_type == "LFP12180":
            return 18
        elif self.batt_type == "CLI18-12":
            return 18
        elif self.batt_type == "CLI20-12":
            return 20
        elif self.batt_type == "RBT50LFP48S-G2":
            return 50

        return 18

class PumpManager(threading.Thread):
    _log = logging.getLogger("pumpman")

    def __init__(self, settings, reporter, devs, **kwargs):
        super().__init__(**kwargs)
        self.settings = settings
        self._reporter = reporter
        self._stop_event = threading.Event()
        self._interrupt_event = threading.Event()

        self._state_lock = threading.Lock()
        self._ntp_has_sync = False
        self._soc_too_low = False
        self._soc_above_thresh = False
        self._schedule_state = False
        self._pump_on = False
        self._load_out_of_range = False
        self._forced_on = False
        self._forced_disable = True
        self._pump_turn_on_time = None
        self._pump_on_time = 0
        self._load_in_range_time = None
        self._batt_full_charge = False
        self._saw_absorption = False
        self._last_charge_state = None

        try:
            self._relay = gpio.Relay(settings.relay)
        except (relay.RelayException, OSError):
            self._relay = gpio.MockRelay()

        self._devs = devs
        self._load_timer = None

        self._devs.registerSampleHandler("batt_shunt", "pump_manager",
                                         self.battShuntSample)
        self._devs.registerSampleHandler("mppt", "pump_manager",
                                         self.mpptSample)
        settings.registerUpdateHandler("pump_manager", self._updateSettings)
        self._reporter.registerTelemetry("pm", self._getTelemetry)

        self._reporter.queueEvent(Events.POWER_ON)

    def _reset_load_error(self):
        with self._state_lock:
            self._log.info("Load Error Reset")
            self._load_out_of_range = False

    def reset_load_error(self):
        self._reset_load_error()
        self.interrupt()

    def _updateSettings(self, new_settings):
        if new_settings.get("reset_load_error", False):
            self._reset_load_error()

        self._reporter.queueEvent(Events.SETTINGS_CHANGE)

        if new_settings.get("restart_mppt", False):
            self._log.info("Restarting MPPT")
            self._devs["mppt"].sendRestart()

        self.interrupt()

    def _elapsed(self):
        now = datetime.datetime.now()
        start = now.replace(minute=0, second=0, microsecond=0)

        if self.settings.sched_period != "hourly":
            start = start.replace(hour=0)

        if self.settings.sched_period == "weekly":
            start -= datetime.timedelta(days=start.weekday())

        return (now - start).total_seconds()

    def _pumpOff(self, reason, event):
        if not self._pump_on:
            return

        self.stopLoadSample()

        self._forced_on = False
        self._pump_on = False
        self._pump_on_time += time.monotonic() - self._pump_turn_on_time
        self._relay.open()
        self._log.info(f"Pump OFF ({reason})")
        self._reporter.queueEvent(event)

    def _pumpOn(self, reason, event):
        if self._pump_on:
            return

        self._pump_on = True
        self._pump_turn_on_time = time.monotonic()
        self._relay.close()
        self._log.info("Pump ON")
        self._reporter.queueEvent(event)

        self._load_timer = threading.Timer(self.settings.load_delay,
                                           self.startLoadSample)
        self._load_timer.start()

    def _setPump(self, state):
        with self._state_lock:
            if state and not self._schedule_state:
                self._pump_on_time = 0
                self._pump_turn_on_time = time.monotonic()

            if not state and self._schedule_state:
                self._batt_full_charge = False

            self._schedule_state = state

            if self._load_out_of_range:
                self._pumpOff("Load Current Out Of Range",
                              Events.PUMP_OFF_LOAD_CURRENT)
                return

            if self.settings.force_pump_off:
                self._pumpOff("Forced Off", Events.PUMP_OFF_FORCE)
                return

            if self._soc_too_low:
                if self._forced_on:
                    self._forced_disable = True
                self._pumpOff("SOC too low", Events.PUMP_OFF_SOC_LOW)
                return

            if self.settings.force_pump_on and not self._forced_disable:
                if not self._forced_on:
                    self._forced_on = True
                    self._pumpOn("Forced On", Events.PUMP_ON_FORCE)
                return

            if not self.settings.force_pump_on:
                self._forced_disable = False

            if self._forced_on and not self.settings.force_pump_on:
                self._pumpOff("Forcing turned Off", Events.PUMP_OFF_FORCE)

            if state:
                if not self._batt_full_charge:
                    self._log.info("Not starting, Battery has not fully charged")
                    return

                if self._soc_above_thresh:
                    self._pumpOn("Schedule Started", Events.PUMP_ON_SCHEDULE)
                elif not self._pump_on:
                    self._soc_too_low = True
                    self._log.info("Not starting, SOC too low")
            else:
                self._pumpOff("Schedule Ended",
                              Events.PUMP_OFF_SCHEDULE)

    def _isNTPSynchronized(self):
        with self._state_lock:
            if self._ntp_has_sync:
                return True

        data = subprocess.check_output(["timedatectl", "show"], text=True)
        for line in data.split():
            k, v = line.strip().split("=")
            if k == "NTPSynchronized":
                with self._state_lock:
                    self._ntp_has_sync = True
                return v == "yes"

    def _syncTime(self):
        self._log.warning("Time not syncronized")
        while not self._isNTPSynchronized():
            self._interrupt_event.wait(15)
            self._interrupt_event.clear()
            if self._stop_event.isSet():
                return

        self._log.info("Time syncronized")

    def _getIntervals(self):
        if self.settings.sched_intervals:
            return self.settings.sched_intervals

        return [(self.settings.schedule_start / 1000,
                 self.settings.schedule_stop / 1000)]

    def _checkDays(self):
        if self.settings.sched_period != "daily":
            return True

        weekday = datetime.date.today().weekday()

        if weekday == 0:
            return self.settings.sched_mon
        elif weekday == 1:
            return self.settings.sched_Tue
        elif weekday == 2:
            return self.settings.sched_wed
        elif weekday == 3:
            return self.settings.sched_thu
        elif weekday == 4:
            return self.settings.sched_fri
        elif weekday == 5:
            return self.settings.sched_sat
        elif weekday == 6:
            return self.settings.sched_sun

    def run(self):
        while not self._stop_event.isSet():
            if not self._isNTPSynchronized():
                self._syncTime()
                continue

            elapsed = self._elapsed()

            on = False
            for start, end in sorted(self._getIntervals()):
                if elapsed < start:
                    next = start
                    break
                if elapsed >= start and elapsed < end:
                    on = self._checkDays()
                    next = end
                    break
            else:
                if self.settings.sched_period == "weekly":
                    next = 24 * 3600 * 7
                elif self.settings.sched_period == "daily":
                    next = 24 * 3600
                elif self.settings.sched_period == "hourly":
                    next = 3600

            self._setPump(on)

            sleep_for = next - elapsed
            self._log.debug(f"Sleep {datetime.timedelta(seconds=round(sleep_for))}")
            self._interrupt_event.wait(sleep_for)
            self._interrupt_event.clear()

    def interrupt(self):
        self._interrupt_event.set()

    def stop(self):
        self._stop_event.set()
        self._pumpOff("Program exited", Events.PUMP_OFF_EXITED)
        self._reporter.queueEvent(Events.SERVICE_EXITED)
        self._interrupt_event.set()
        self.join()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        self.join()

    def battShuntSample(self, sample):
        soc = sample.get("SOC", None)
        if soc is None:
            return

        with self._state_lock:
            if not self._soc_above_thresh and soc >= self.settings.turn_on_soc:
                self._log.info("SOC above threshold")
                self._soc_above_thresh = True
                self.interrupt()
            elif self._soc_above_thresh and soc < self.settings.turn_on_soc:
                self._soc_above_thresh = False

            if self._soc_too_low and soc >= self.settings.turn_on_soc:
                self._soc_too_low = False
            elif not self._soc_too_low and soc <= self.settings.min_soc:
                self._log.info("SOC below threshold")
                self._soc_too_low = True
                self.interrupt()

    def mpptSample(self, sample):
        cs = sample.get("CS", None)
        v = sample.get("V", None)

        if sample.get("ERR", 0) != 0:
            self._reporter.queueEvent(Events.MPPT_ERROR)

        if (cs in (MPPTChargeState.ABSORPTION, MPPTChargeState.FLOAT) and
            v >= settings.getAbsThresh()):
            if not self._saw_absorption:
                self._log.info(f"Absorption State ({v})")
                self._reporter.queueEvent(Events.MPPT_ABSORPTION_STATE)
            self._saw_absorption = True

        if (cs != self._last_charge_state and cs == MPPTChargeState.FLOAT and
              self._saw_absorption):
            self._log.info("Syncing Battery Shunt")
            cmd = vedirect.VEDirectParam(0x102C, "cmd")
            self._devs.setParam("batt_shunt", cmd)
            self._reporter.queueEvent(Events.BATT_FULL_CHARGE)
        elif cs not in (MPPTChargeState.ABSORPTION, MPPTChargeState.FLOAT):
            if self._saw_absorption:
                self._log.info("Exiting Float State")
            self._saw_absorption = False

        self._last_charge_state = cs

        with self._state_lock:
            if (cs == MPPTChargeState.FLOAT and not self._batt_full_charge and
                self._saw_absorption):
                self._log.info("Battery Fully Charged")
                self._batt_full_charge = True
                self.interrupt()

    def startLoadSample(self):
        self._log.debug(f"Monitoring Load Current")
        self._devs.registerSampleHandler("load_shunt", "load_sample",
                                         self.loadSample)

    def stopLoadSample(self):
        self._log.debug(f"Disabling Load Current Monitoring")

        if self._load_timer is not None:
            self._load_timer.cancel()

        self._devs.unregisterSampleHandler("load_shunt", "load_sample")

    def loadSample(self, sample):
        current = sample.get("I", None)
        if current is None:
            return

        voltage = sample.get("V", None)
        if voltage is None:
            return

        if voltage < self.settings.load_min_voltage:
            self._load_in_range_time = None
            return

        if (current >= self.settings.min_load and
            current <= self.settings.max_load):
            self._load_in_range_time = None
            return

        if self._load_in_range_time is None:
            self._load_in_range_time = time.monotonic()
            return

        delay = time.monotonic() - self._load_in_range_time

        if delay < self.settings.load_detect:
            return

        with self._state_lock:
            if not self._load_out_of_range:
                if current < self.settings.min_load:
                    self._log.warning(f"Load current is too low: {current}mA {voltage}mV {delay:.1f}s")
                    self._load_out_of_range = True
                    self.interrupt()
                elif current > self.settings.max_load:
                    self._log.warning(f"Load current is too high: {current}mA {voltage}mV {delay:.1f}s")
                    self._load_out_of_range = True
                    self.interrupt()

    def _calcPumpOnTime(self):
        on_time = self._pump_on_time
        if self._pump_on and self._pump_turn_on_time is not None:
            on_time += time.monotonic() - self._pump_turn_on_time

        return on_time

    def _getTelemetry(self):
        with self._state_lock:
            ret = {
                "pump_is_on":           self._pump_on,
                "schedule":             self._schedule_state,
                "soc_too_low":          self._soc_too_low,
                "full_charge":          self._batt_full_charge,
                "load_out_of_range":    self._load_out_of_range,
                "forced_on":            self._forced_on,
                "forced_off":           self.settings.force_pump_off,
            }

            if self._ntp_has_sync:
                ret["on_time"] = self._calcPumpOnTime()
                ret["time"] = time.time()
                ret["localtime"] = time.strftime("%Y-%m-%d %a %H:%M:%S %Z")

            return ret

class GPSManager:
    def __init__(self, settings, reporter):
        self._settings = settings
        self._reporter = reporter
        self._gps = gpsmon.GPSMonitor(settings.gpsd_server)

        settings.registerUpdateHandler("gps_manager", self._updateSettings)
        self._reporter.registerTelemetry("gps", self._getTelemetry)
        self._updateSettings()
        self._gps.registerFixCallback("gps_manager", self._gotFix)

    def _updateSettings(self, new_settings={}):
        self._gps.setSpeedThresh(self._settings.gps_speed_thresh,
                                 self._threshExceeded)
    def _threshExceeded(self):
        self._reporter.queueEvent(Events.GPS_SPEED_EXCEEDED)

    def _getTelemetry(self):
        return self._gps.getLatestSample()

    def _gotFix(self):
        self._reporter.queueEvent(Events.GPS_GOT_FIX)

    def __enter__(self):
        self._gps.__enter__()
        return self

    def __exit__(self, *args, **kws):
        self._gps.__exit__(*args, **kws)

class InputMonitor:
    _log = logging.getLogger("input")
    _input_names = ["tank_too_low", "tank_refill", "leak_detect"]

    def __init__(self, settings, reporter, pm):
        self._settings = settings
        self._reporter = reporter
        self._pm = pm

        self._inputs = {}
        for i in self._input_names:
            inp_num = getattr(settings, "inp_" + i, None)
            if inp_num is None or inp_num <= 0:
                continue
            inp = gpio.Input(inp_num, self._event)
            inp.name = i
            self._inputs[i] = inp

        self._reporter.registerTelemetry("inp", self._getTelemetry)

    def _getTelemetry(self):
        return {name: inp.state() == "closed"
                for name, inp in self._inputs.items()}

    def _event(self, inp, state):
        self._log.info(f"Input Event: {inp.name} {state}")
        if inp.name == "tank_refill" and inp.state() == "open":
            self._log.info("Tank Filled")
            self._pm.reset_load_error()
        self._reporter.queueEvent(Events.INPUT_CHANGED)

    def __enter__(self):
        for _, inp in self._inputs.items():
            inp.__enter__()

        return self

    def __exit__(self, *args, **kws):
        for _, inp in self._inputs.items():
            inp.__exit__(*args, **kws)

class TempMonitor:
    _log = logging.getLogger("tempmon")

    def __init__(self, reporter, mppt):
        self._reporter = reporter
        self._mppt = mppt

        self._reporter.registerTelemetry("temp", self._getTelemetry)

    def _getTelemetry(self):
        try:
            if self._mppt.stop.is_set():
                return {}
            temp = self._mppt.getParam(vedirect.VEDirectParam(0xEDDB, "sn16"))
            self._log.debug(f"Temperature: {temp/100}")
            return {"MPPT": temp}
        except vedirect.VEDirectException as e:
            self._log.error(str(e))
            return {"ERROR": str(e)}

def git(*args):
    return subprocess.check_output(["git"] + list(args), cwd=ROOT,
                                   text=True, stderr=subprocess.STDOUT)

def update(version=None):
    log = logging.getLogger("update")
    log.info(f"Starting update version={version}")

    resp = {}
    resp["fetch"] = git("fetch", "--prune", "--all")

    if version is None:
        resp["pull"] = git("pull")
    else:
        resp["reset"] = git("reset", "--hard", version)

    resp["deploy"] = subprocess.check_output(["./deploy", "-i"],
                                             cwd=ROOT, text=True)

    return resp

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ASIS Main Server")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="enable extra log messages")
    parser.add_argument("--timestamp", "-t", action="store_true",
                        help="add timestamp to log messages")
    parser.add_argument("--config", "-c", metavar="CFG",
                        type=argparse.FileType('r'),
                        help="Specify config JSON file to add to the current settings")
    parser.add_argument("--username", "--access-token", "-u", metavar="ID",
                        help="MQTT username to use (ie thingsboard access token)")
    parser.add_argument("--username-file", "--access-token-file", "-U",
                        type=argparse.FileType('r'),
                        help="MQTT username to be read from the specfied file")
    parser.add_argument("--no-devs", action="store_true",
                        help="Don't try to connect to any VEdirect devices")
    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    log_fmt = "%(name)-8s %(message)s"
    if args.timestamp:
        log_fmt = "%(asctime)-15s " + log_fmt
    logging.basicConfig(level=log_level, format=log_fmt)

    log = logging.getLogger("init")

    if args.username_file:
        username = args.username_file.read().strip()
    else:
        username = args.username

    if username is None:
        print("Must specify a username (with -u)", file=sys.stderr)
        sys.exit(1)

    settings = Settings()
    try:
        if args.config:
            settings.updateFromFile(args.config)
    except InvalidSetting as e:
        print("Invalid Setting:", e, file=sys.stderr)
        sys.exit(1)

    vecfg = ROOT / "vecfg" / f"vedirect-{settings.batt_type.lower()}.cfg"
    if not vecfg.exists():
        print("Unknown battery type:", settings.batt_type, file=sys.stderr)
        sys.exit(1)

    log.info(f"Loading vecfg from {vecfg}")
    with vecfg.open() as f:
        cfg = configparser.ConfigParser(inline_comment_prefixes="#")
        cfg.read_file(f)

    cfg.set("batt_shunt", "0x1000,Un16", str(settings.getBattCap()))

    rep = None

    if not args.no_devs:
        devs = pathlib.Path("/dev").glob("ttyUSB*")
    else:
        devs = []

    try:
        with contextlib.ExitStack() as stack:
            devs = vedirect.VEDirectList(devs, cfg=cfg, exit_stack=stack, log=log)

            rep = reporter.Reporter(settings, devs, username=username)
            rep.registerRPC("update", update)

            log.info(f"Starting GPS Manager")
            gps = GPSManager(settings, rep)
            stack.enter_context(gps)

            log.info(f"Starting Temp Monitor")
            tm = TempMonitor(rep, devs["mppt"])

            log.info(f"Starting Pump Manager")
            pm = PumpManager(settings, rep, devs)
            stack.enter_context(pm)

            log.info("Starting Input Monitor")
            inp = InputMonitor(settings, rep, pm)
            stack.enter_context(inp)

            log.info(f"Starting Reporter Loop")
            rep.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        if rep:
            rep.send_final_events()
