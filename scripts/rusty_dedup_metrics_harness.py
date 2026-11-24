#!/usr/bin/env python3
"""
rusty_dedup_metrics_harness.py — Zero-Dependency Companion Metrics Harness for rusty_dedup.

Combines three observability layers into a single Prometheus v0.0.4 `/metrics` endpoint
(sharing `dedup_config.yaml` as the single source of truth for Prometheus settings):

  1. OS-Kernel Thread & Child Process Introspection (via native C syscalls, no root/admin):
     - macOS   : ctypes -> /usr/lib/libSystem.B.dylib (`proc_pidinfo` PROC_PIDLISTTHREADS /
                 PROC_PIDTHREADINFO, `proc_pid_rusage` RUSAGE_INFO_V4, `proc_listchildpids`)
     - Linux   : /proc/<pid>/task/<tid>/{comm,stat,io,wchan} + child tree
     - Windows : ctypes -> kernel32.dll (`CreateToolhelp32Snapshot`, `OpenThread`,
                 `GetThreadTimes`, `GetThreadDescription`, `GetProcessIoCounters`)

  2. Host Hardware Syscall Sampler (reused from rusty_h265_metrics_agent.py):
     - CPU user/system/idle/active ratios (Mach host_statistics / /proc/stat / GetSystemTimes)
     - Host Load Average (1m / 5m / 15m) & VM Memory (used/free/total/wired/compressed)
     - Apple Silicon IOAccelerator GPU / NVIDIA NVML utilization, VRAM, temperature, power

  3. Rust Application-Domain Telemetry Snapshot Merger:
     - Reads the atomic JSON snapshot flushed every 500ms by `rusty_dedup`
       (`/tmp/rusty_dedup_telemetry_<pid>.json`) covering decode realtime factors, 24-bucket
       latency histograms, LRU caches, SQLite3 WAL commits, duplicate funnel, and ffprobe
       bitrate extraction sources.
"""

import argparse
import ctypes
import http.server
import json
import os
import platform
import re
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, Tuple


# ============================================================================
# 1. Config Loading (`dedup_config.yaml` Single Source of Truth)
# ============================================================================

def detect_host_os_label() -> str:
    sys_name = platform.system().lower()
    if "darwin" in sys_name or "mac" in sys_name:
        return "Mac"
    if "win" in sys_name:
        return "Windows"
    return "Linux"


def _strip_quotes(val: str) -> str:
    v = val.strip()
    if len(v) >= 2 and ((v[0] == '"' and v[-1] == '"') or (v[0] == "'" and v[-1] == "'")):
        return v[1:-1]
    return v


def _parse_optional_str(val: str) -> Optional[str]:
    v = _strip_quotes(val).strip()
    if not v or v.lower() in ("none", "null", "~", "off", "disabled"):
        return None
    return v


def load_dedup_config_yaml(path: Optional[str]) -> Dict[str, Any]:
    """Parses `telemetry` and `telemetry.prometheus` sections from `dedup_config.yaml`."""
    cfg: Dict[str, Any] = {
        "enabled": True,
        "heartbeat_interval_secs": 30,
        "snapshot_path": None,
        "listen_addr": None,
        "pushgateway_url": None,
        "push_interval_secs": 15,
        "job_name": "rusty_dedup",
        "textfile_path": None,
    }
    if not path or not os.path.isfile(path):
        return cfg

    section = "root"
    telemetry_indent = 0
    prom_indent = 0
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for raw_line in f:
                code = raw_line.split("#", 1)[0]
                trimmed = code.strip()
                if not trimmed:
                    continue
                indent = len(code) - len(code.lstrip())
                if section == "prometheus" and indent <= prom_indent:
                    section = "root" if indent <= telemetry_indent else "telemetry"
                elif section == "telemetry" and indent <= telemetry_indent:
                    section = "root"

                if ":" not in trimmed or trimmed.startswith("-"):
                    continue
                k_raw, v_raw = trimmed.split(":", 1)
                k = _strip_quotes(k_raw).strip()
                v = _strip_quotes(v_raw).strip()

                if section == "root":
                    if k == "telemetry":
                        telemetry_indent = indent
                        if not v:
                            section = "telemetry"
                        else:
                            cfg["enabled"] = v.lower() in ("true", "yes", "on", "1")
                elif section == "telemetry":
                    if k == "enabled":
                        cfg["enabled"] = v.lower() in ("true", "yes", "on", "1")
                    elif k == "heartbeat_interval_secs" and v.isdigit():
                        cfg["heartbeat_interval_secs"] = int(v)
                    elif k == "snapshot_path":
                        cfg["snapshot_path"] = _parse_optional_str(v)
                    elif k == "prometheus":
                        prom_indent = indent
                        section = "prometheus"
                elif section == "prometheus":
                    if k == "listen_addr":
                        cfg["listen_addr"] = _parse_optional_str(v)
                    elif k == "pushgateway_url":
                        cfg["pushgateway_url"] = _parse_optional_str(v)
                    elif k == "push_interval_secs" and v.isdigit():
                        cfg["push_interval_secs"] = max(1, int(v))
                    elif k == "job_name":
                        parsed = _parse_optional_str(v)
                        if parsed:
                            cfg["job_name"] = parsed
                    elif k == "textfile_path":
                        cfg["textfile_path"] = _parse_optional_str(v)
    except Exception:
        pass
    return cfg


def resolve_default_config_path(explicit: Optional[str]) -> Optional[str]:
    if explicit:
        return explicit
    script_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(os.getcwd(), "dedup_config.yaml"),
        os.path.join(os.path.dirname(script_dir), "dedup_config.yaml"),
    ]
    for cand in candidates:
        if os.path.isfile(cand):
            return cand
    return None


# ============================================================================
# 2. OS-Level Thread & Child Process Introspection (macOS / Linux / Windows)
# ============================================================================

class MacProcThreadInfo(ctypes.Structure):
    """Matches `<sys/proc_info.h> struct proc_threadinfo` on 64-bit macOS."""
    _fields_ = [
        ("pth_user_time", ctypes.c_uint64),
        ("pth_system_time", ctypes.c_uint64),
        ("pth_cpu_usage", ctypes.c_int32),
        ("pth_policy", ctypes.c_int32),
        ("pth_run_state", ctypes.c_int32),
        ("pth_flags", ctypes.c_int32),
        ("pth_sleep_time", ctypes.c_int32),
        ("pth_curpri", ctypes.c_int32),
        ("pth_priority", ctypes.c_int32),
        ("pth_maxpriority", ctypes.c_int32),
        ("pth_name", ctypes.c_char * 64),
    ]


class MacRusageInfoV4(ctypes.Structure):
    """Matches `<sys/resource.h> struct rusage_info_v4` on 64-bit macOS."""
    _fields_ = [
        ("ri_uuid", ctypes.c_uint8 * 16),
        ("ri_user_time", ctypes.c_uint64),
        ("ri_system_time", ctypes.c_uint64),
        ("ri_pkg_idle_wkups", ctypes.c_uint64),
        ("ri_interrupt_wkups", ctypes.c_uint64),
        ("ri_pageins", ctypes.c_uint64),
        ("ri_wired_size", ctypes.c_uint64),
        ("ri_resident_size", ctypes.c_uint64),
        ("ri_phys_footprint", ctypes.c_uint64),
        ("ri_proc_start_abstime", ctypes.c_uint64),
        ("ri_proc_exit_abstime", ctypes.c_uint64),
        ("ri_child_user_time", ctypes.c_uint64),
        ("ri_child_system_time", ctypes.c_uint64),
        ("ri_child_pkg_idle_wkups", ctypes.c_uint64),
        ("ri_child_interrupt_wkups", ctypes.c_uint64),
        ("ri_child_pageins", ctypes.c_uint64),
        ("ri_child_elapsed_abstime", ctypes.c_uint64),
        ("ri_diskio_bytesread", ctypes.c_uint64),
        ("ri_diskio_byteswritten", ctypes.c_uint64),
        ("ri_cpu_time_qos_default", ctypes.c_uint64),
        ("ri_cpu_time_qos_maintenance", ctypes.c_uint64),
        ("ri_cpu_time_qos_background", ctypes.c_uint64),
        ("ri_cpu_time_qos_utility", ctypes.c_uint64),
        ("ri_cpu_time_qos_legacy", ctypes.c_uint64),
        ("ri_cpu_time_qos_user_initiated", ctypes.c_uint64),
        ("ri_cpu_time_qos_user_interactive", ctypes.c_uint64),
        ("ri_billed_system_time", ctypes.c_uint64),
        ("ri_serviced_system_time", ctypes.c_uint64),
        ("ri_logical_writes", ctypes.c_uint64),
        ("ri_lifetime_max_phys_footprint", ctypes.c_uint64),
        ("ri_instructions", ctypes.c_uint64),
        ("ri_cycles", ctypes.c_uint64),
        ("ri_billed_energy", ctypes.c_uint64),
        ("ri_serviced_energy", ctypes.c_uint64),
        ("ri_interval_max_phys_footprint", ctypes.c_uint64),
        ("ri_runnable_time", ctypes.c_uint64),
    ]


class ProcessThreadIntrospector:
    """
    Externally inspects a target PID's OS threads & child processes (`ffmpeg`/`ffprobe`)
    using unprivileged native OS syscalls (macOS libproc, Linux procfs, Windows Win32).
    """

    def __init__(self) -> None:
        self.host_os = detect_host_os_label()
        self._libc: Optional[ctypes.CDLL] = None
        self._clk_tck: float = 100.0
        self._page_size: int = 4096
        if self.host_os == "Mac":
            try:
                self._libc = ctypes.CDLL("/usr/lib/libSystem.B.dylib")
            except Exception:
                self._libc = None
        elif self.host_os == "Linux":
            try:
                self._clk_tck = float(os.sysconf("SC_CLK_TCK"))
            except Exception:
                self._clk_tck = 100.0
            try:
                self._page_size = int(os.sysconf("SC_PAGE_SIZE"))
            except Exception:
                self._page_size = 4096

    def _mac_proc_name(self, pid: int) -> str:
        if not self._libc:
            return ""
        buf = ctypes.create_string_buffer(64)
        try:
            n = self._libc.proc_name(ctypes.c_int(pid), buf, ctypes.c_uint32(64))
            if n > 0:
                return buf.value.decode("utf-8", errors="replace").strip()
        except Exception:
            pass
        return ""

    def _mac_rusage(self, pid: int) -> Optional[Dict[str, Any]]:
        if not self._libc:
            return None
        ru = MacRusageInfoV4()
        try:
            # RUSAGE_INFO_V4 = 4
            rc = self._libc.proc_pid_rusage(ctypes.c_int(pid), ctypes.c_int(4), ctypes.byref(ru))
            if rc == 0:
                return {
                    "rss_bytes": int(ru.ri_resident_size),
                    "phys_footprint_bytes": int(ru.ri_phys_footprint),
                    "disk_read_bytes": int(ru.ri_diskio_bytesread),
                    "disk_write_bytes": int(ru.ri_diskio_byteswritten),
                    "user_secs": float(ru.ri_user_time) / 1e9,
                    "sys_secs": float(ru.ri_system_time) / 1e9,
                }
        except Exception:
            pass
        return None

    def _mac_list_children(self, pid: int) -> List[int]:
        if not self._libc:
            return []
        buf = (ctypes.c_int32 * 128)()
        try:
            nbytes = self._libc.proc_listchildpids(
                ctypes.c_int(pid), ctypes.byref(buf), ctypes.c_int(ctypes.sizeof(buf))
            )
            if nbytes > 0:
                count = nbytes // 4
                return [int(buf[i]) for i in range(count) if buf[i] > 0]
        except Exception:
            pass
        return []

    def sample_target_process(self, pid: int) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "pid": pid,
            "alive": False,
            "threads": [],
            "main_proc": {},
            "children_summary": {},  # role -> {count, rss_bytes, disk_read_bytes, disk_write_bytes}
        }
        if pid <= 0:
            return result

        if self.host_os == "Mac" and self._libc is not None:
            # PROC_PIDLISTTHREADS = 6, PROC_PIDTHREADINFO = 5
            tids = (ctypes.c_uint64 * 256)()
            nbytes = self._libc.proc_pidinfo(
                ctypes.c_int(pid),
                ctypes.c_int(6),
                ctypes.c_uint64(0),
                ctypes.byref(tids),
                ctypes.c_int(ctypes.sizeof(tids)),
            )
            if nbytes <= 0:
                return result
            result["alive"] = True
            ntids = min(256, nbytes // 8)
            for i in range(ntids):
                tid = int(tids[i])
                info = MacProcThreadInfo()
                r = self._libc.proc_pidinfo(
                    ctypes.c_int(pid),
                    ctypes.c_int(5),
                    ctypes.c_uint64(tid),
                    ctypes.byref(info),
                    ctypes.c_int(ctypes.sizeof(info)),
                )
                if r <= 0:
                    continue
                tname = info.pth_name.decode("utf-8", errors="replace").strip("\x00").strip()
                if not tname:
                    tname = "main" if i == 0 else f"thread_{tid}"
                # TH_STATE_RUNNING=1, STOPPED=2, WAITING=3, UNINTERRUPTIBLE=4, HALTED=5
                state_map = {1: "running", 2: "stopped", 3: "waiting", 4: "disk_io", 5: "halted"}
                result["threads"].append(
                    {
                        "tid": str(tid),
                        "name": tname,
                        "user_secs": float(info.pth_user_time) / 1e9,
                        "sys_secs": float(info.pth_system_time) / 1e9,
                        "cpu_ratio": max(0.0, float(info.pth_cpu_usage) / 1000.0),
                        "state": state_map.get(int(info.pth_run_state), "other"),
                    }
                )

            main_ru = self._mac_rusage(pid)
            if main_ru:
                result["main_proc"] = main_ru

            children: Dict[str, Dict[str, int]] = {}
            for cpid in self._mac_list_children(pid):
                cname = self._mac_proc_name(cpid).lower() or "child"
                role = "ffmpeg" if "ffmpeg" in cname else ("ffprobe" if "ffprobe" in cname else cname)
                cru = self._mac_rusage(cpid) or {}
                agg = children.setdefault(
                    role, {"count": 0, "rss_bytes": 0, "disk_read_bytes": 0, "disk_write_bytes": 0}
                )
                agg["count"] += 1
                agg["rss_bytes"] += int(cru.get("rss_bytes", 0))
                agg["disk_read_bytes"] += int(cru.get("disk_read_bytes", 0))
                agg["disk_write_bytes"] += int(cru.get("disk_write_bytes", 0))
            result["children_summary"] = children
            return result

        if self.host_os == "Linux":
            task_dir = f"/proc/{pid}/task"
            if not os.path.isdir(task_dir):
                return result
            result["alive"] = True
            child_pids: List[int] = []
            try:
                for tid_str in sorted(os.listdir(task_dir)):
                    tpath = os.path.join(task_dir, tid_str)
                    try:
                        with open(os.path.join(tpath, "comm"), "r", encoding="utf-8", errors="replace") as f:
                            tname = f.read().strip() or f"thread_{tid_str}"
                        with open(os.path.join(tpath, "stat"), "r", encoding="utf-8", errors="replace") as f:
                            stat_raw = f.read()
                        rparen = stat_raw.rfind(")")
                        fields = stat_raw[rparen + 2:].split() if rparen != -1 else []
                        st_ch = fields[0] if fields else "?"
                        utime = int(fields[11]) / self._clk_tck if len(fields) > 12 else 0.0
                        stime = int(fields[12]) / self._clk_tck if len(fields) > 12 else 0.0
                        st_map = {"R": "running", "S": "waiting", "D": "disk_io", "T": "stopped", "Z": "halted"}
                        result["threads"].append(
                            {
                                "tid": tid_str,
                                "name": tname,
                                "user_secs": utime,
                                "sys_secs": stime,
                                "cpu_ratio": 0.0,
                                "state": st_map.get(st_ch, "other"),
                            }
                        )
                        ch_file = os.path.join(tpath, "children")
                        if os.path.isfile(ch_file):
                            with open(ch_file, "r", encoding="utf-8", errors="replace") as cf:
                                for tok in cf.read().split():
                                    if tok.isdigit():
                                        child_pids.append(int(tok))
                    except Exception:
                        continue
            except Exception:
                pass

            def _linux_proc_stats(target_pid: int) -> Dict[str, int]:
                out = {"rss_bytes": 0, "disk_read_bytes": 0, "disk_write_bytes": 0}
                try:
                    with open(f"/proc/{target_pid}/statm", "r", encoding="utf-8") as f:
                        parts = f.read().split()
                        if len(parts) >= 2:
                            out["rss_bytes"] = int(parts[1]) * self._page_size
                except Exception:
                    pass
                try:
                    with open(f"/proc/{target_pid}/io", "r", encoding="utf-8") as f:
                        for line in f:
                            if line.startswith("read_bytes:"):
                                out["disk_read_bytes"] = int(line.split(":", 1)[1].strip())
                            elif line.startswith("write_bytes:"):
                                out["disk_write_bytes"] = int(line.split(":", 1)[1].strip())
                except Exception:
                    pass
                return out

            result["main_proc"] = _linux_proc_stats(pid)
            children_l: Dict[str, Dict[str, int]] = {}
            for cpid in set(child_pids):
                try:
                    with open(f"/proc/{cpid}/comm", "r", encoding="utf-8", errors="replace") as f:
                        cname = f.read().strip().lower()
                except Exception:
                    continue
                role = "ffmpeg" if "ffmpeg" in cname else ("ffprobe" if "ffprobe" in cname else cname)
                cs = _linux_proc_stats(cpid)
                agg = children_l.setdefault(
                    role, {"count": 0, "rss_bytes": 0, "disk_read_bytes": 0, "disk_write_bytes": 0}
                )
                agg["count"] += 1
                agg["rss_bytes"] += cs["rss_bytes"]
                agg["disk_read_bytes"] += cs["disk_read_bytes"]
                agg["disk_write_bytes"] += cs["disk_write_bytes"]
            result["children_summary"] = children_l
            return result

        if self.host_os == "Windows":
            try:
                k32 = ctypes.windll.kernel32

                class THREADENTRY32(ctypes.Structure):
                    _fields_ = [
                        ("dwSize", ctypes.c_uint32),
                        ("cntUsage", ctypes.c_uint32),
                        ("th32ThreadID", ctypes.c_uint32),
                        ("th32OwnerProcessID", ctypes.c_uint32),
                        ("tpBasePri", ctypes.c_long),
                        ("tpDeltaPri", ctypes.c_long),
                        ("dwFlags", ctypes.c_uint32),
                    ]

                class FILETIME(ctypes.Structure):
                    _fields_ = [("dwLowDateTime", ctypes.c_uint32), ("dwHighDateTime", ctypes.c_uint32)]

                # TH32CS_SNAPTHREAD = 0x00000004
                snap = k32.CreateToolhelp32Snapshot(0x00000004, 0)
                if snap == ctypes.c_void_p(-1).value:
                    return result
                try:
                    te = THREADENTRY32()
                    te.dwSize = ctypes.sizeof(THREADENTRY32)
                    ok = k32.Thread32First(snap, ctypes.byref(te))
                    while ok:
                        if te.th32OwnerProcessID == pid:
                            result["alive"] = True
                            tid = int(te.th32ThreadID)
                            tname = f"thread_{tid}"
                            u_secs = 0.0
                            s_secs = 0.0
                            # THREAD_QUERY_LIMITED_INFORMATION = 0x0800
                            hth = k32.OpenThread(0x0800, False, tid)
                            if hth:
                                try:
                                    c_ft, e_ft, k_ft, u_ft = FILETIME(), FILETIME(), FILETIME(), FILETIME()
                                    if k32.GetThreadTimes(
                                        hth, ctypes.byref(c_ft), ctypes.byref(e_ft), ctypes.byref(k_ft), ctypes.byref(u_ft)
                                    ):
                                        u_secs = ((u_ft.dwHighDateTime << 32) | u_ft.dwLowDateTime) / 1e7
                                        s_secs = ((k_ft.dwHighDateTime << 32) | k_ft.dwLowDateTime) / 1e7
                                    desc_ptr = ctypes.c_wchar_p()
                                    if hasattr(k32, "GetThreadDescription"):
                                        hr = k32.GetThreadDescription(hth, ctypes.byref(desc_ptr))
                                        if hr == 0 and desc_ptr.value:
                                            tname = desc_ptr.value.strip()
                                        if desc_ptr:
                                            k32.LocalFree(desc_ptr)
                                finally:
                                    k32.CloseHandle(hth)
                            result["threads"].append(
                                {
                                    "tid": str(tid),
                                    "name": tname,
                                    "user_secs": u_secs,
                                    "sys_secs": s_secs,
                                    "cpu_ratio": 0.0,
                                    "state": "running",
                                }
                            )
                        ok = k32.Thread32Next(snap, ctypes.byref(te))
                finally:
                    k32.CloseHandle(snap)
            except Exception:
                pass
            return result

        return result


# ============================================================================
# 3. Host Hardware Syscall Samplers (reused from rusty_h265_metrics_agent.py)
# ============================================================================

class CpuTickTracker:
    """Computes host CPU user/system/idle/active ratios using OS syscalls without subprocesses."""

    def __init__(self) -> None:
        self.prev_ticks: Optional[Tuple[int, int, int, int]] = None
        self.host_os = detect_host_os_label()

    def _read_raw_ticks(self) -> Optional[Tuple[int, int, int, int]]:
        if self.host_os == "Mac":
            try:
                libc = ctypes.CDLL("/usr/lib/libSystem.B.dylib")
                host_port = libc.mach_host_self()
                ticks = (ctypes.c_uint32 * 4)()
                count = ctypes.c_uint32(4)
                if libc.host_statistics(host_port, ctypes.c_int(3), ctypes.byref(ticks), ctypes.byref(count)) == 0:
                    return (int(ticks[0]), int(ticks[1]), int(ticks[2]), int(ticks[3]))
            except Exception:
                pass
        elif self.host_os == "Linux":
            try:
                with open("/proc/stat", "r", encoding="utf-8") as f:
                    parts = f.readline().split()
                if len(parts) >= 5 and parts[0] == "cpu":
                    user, nice, system, idle = int(parts[1]), int(parts[2]), int(parts[3]), int(parts[4])
                    iowait = int(parts[5]) if len(parts) > 5 else 0
                    return (user, system + iowait, idle, nice)
            except Exception:
                pass
        elif self.host_os == "Windows":
            try:
                class FILETIME(ctypes.Structure):
                    _fields_ = [("dwLowDateTime", ctypes.c_uint32), ("dwHighDateTime", ctypes.c_uint32)]
                idle_ft, kernel_ft, user_ft = FILETIME(), FILETIME(), FILETIME()
                if ctypes.windll.kernel32.GetSystemTimes(
                    ctypes.byref(idle_ft), ctypes.byref(kernel_ft), ctypes.byref(user_ft)
                ):
                    idle = (idle_ft.dwHighDateTime << 32) | idle_ft.dwLowDateTime
                    kernel = (kernel_ft.dwHighDateTime << 32) | kernel_ft.dwLowDateTime
                    user = (user_ft.dwHighDateTime << 32) | user_ft.dwLowDateTime
                    return (user, max(0, kernel - idle), idle, 0)
            except Exception:
                pass
        return None

    def sample_cpu_ratios(self) -> Tuple[float, float, float, float]:
        cur = self._read_raw_ticks()
        if cur is None:
            return (0.0, 0.0, 1.0, 0.0)
        if self.prev_ticks is None:
            self.prev_ticks = cur
            time.sleep(0.05)
            cur2 = self._read_raw_ticks()
            if cur2 is not None and cur2 != cur:
                cur = cur2
            else:
                tot = sum(cur)
                if tot > 0:
                    u = (cur[0] + cur[3]) / tot
                    s = cur[1] / tot
                    i = cur[2] / tot
                    return (u, s, i, min(1.0, u + s))
                return (0.0, 0.0, 1.0, 0.0)
        prev = self.prev_ticks
        self.prev_ticks = cur
        du = max(0, (cur[0] - prev[0]) + (cur[3] - prev[3]))
        ds = max(0, cur[1] - prev[1])
        di = max(0, cur[2] - prev[2])
        dt = du + ds + di
        if dt <= 0:
            return (0.0, 0.0, 1.0, 0.0)
        return (min(1.0, du / dt), min(1.0, ds / dt), min(1.0, di / dt), min(1.0, (du + ds) / dt))


def get_load_averages_syscall() -> Tuple[float, float, float]:
    if hasattr(os, "getloadavg"):
        try:
            la = os.getloadavg()
            return (float(la[0]), float(la[1]), float(la[2]))
        except Exception:
            pass
    return (0.0, 0.0, 0.0)


def get_memory_stats_gb_syscall() -> Tuple[float, float, float, float, float]:
    host_os = detect_host_os_label()
    gb = 1024.0 * 1024.0 * 1024.0
    if host_os == "Mac":
        try:
            libc = ctypes.CDLL("/usr/lib/libSystem.B.dylib")
            memsize = ctypes.c_uint64(0)
            sz = ctypes.c_size_t(ctypes.sizeof(memsize))
            libc.sysctlbyname(b"hw.memsize", ctypes.byref(memsize), ctypes.byref(sz), None, ctypes.c_size_t(0))
            total_b = memsize.value
            page_size = ctypes.c_uint64(0)
            sz_pg = ctypes.c_size_t(ctypes.sizeof(page_size))
            libc.sysctlbyname(b"hw.pagesize", ctypes.byref(page_size), ctypes.byref(sz_pg), None, ctypes.c_size_t(0))
            pg = page_size.value if page_size.value > 0 else 16384
            vm_buf = (ctypes.c_uint32 * 48)()
            count = ctypes.c_uint32(38)
            kr = libc.host_statistics64(libc.mach_host_self(), ctypes.c_int(4), ctypes.byref(vm_buf), ctypes.byref(count))
            if kr == 0 and total_b > 0:
                free_b = (vm_buf[0] + vm_buf[2]) * pg
                used_b = max(0, total_b - free_b)
                wired_b = vm_buf[3] * pg
                comp_b = (vm_buf[32] if count.value > 32 else 0) * pg
                return (used_b / gb, free_b / gb, total_b / gb, wired_b / gb, comp_b / gb)
        except Exception:
            pass
    elif host_os == "Linux":
        try:
            mem_info: Dict[str, int] = {}
            with open("/proc/meminfo", "r", encoding="utf-8") as f:
                for line in f:
                    if ":" in line:
                        k, v = line.split(":", 1)
                        toks = v.strip().split()
                        if toks and toks[0].isdigit():
                            mem_info[k.strip()] = int(toks[0]) * 1024
            total_b = mem_info.get("MemTotal", 0)
            avail_b = mem_info.get("MemAvailable", mem_info.get("MemFree", 0))
            return (max(0, total_b - avail_b) / gb, avail_b / gb, total_b / gb, 0.0, 0.0)
        except Exception:
            pass
    elif host_os == "Windows":
        try:
            class MEMORYSTATUSEX(ctypes.Structure):
                _fields_ = [
                    ("dwLength", ctypes.c_uint32),
                    ("dwMemoryLoad", ctypes.c_uint32),
                    ("ullTotalPhys", ctypes.c_uint64),
                    ("ullAvailPhys", ctypes.c_uint64),
                    ("ullTotalPageFile", ctypes.c_uint64),
                    ("ullAvailPageFile", ctypes.c_uint64),
                    ("ullTotalVirtual", ctypes.c_uint64),
                    ("ullAvailVirtual", ctypes.c_uint64),
                    ("ullAvailExtendedVirtual", ctypes.c_uint64),
                ]
            st = MEMORYSTATUSEX()
            st.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
            if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(st)):
                return (max(0, st.ullTotalPhys - st.ullAvailPhys) / gb, st.ullAvailPhys / gb, st.ullTotalPhys / gb, 0.0, 0.0)
        except Exception:
            pass
    return (0.0, 0.0, 0.0, 0.0, 0.0)


def sample_gpu_stats_syscall() -> Dict[str, Any]:
    res: Dict[str, Any] = {
        "gpu_name": "",
        "gpu_core_utilization_ratio": 0.0,
        "gpu_decoder_utilization_ratio": 0.0,
        "gpu_renderer_utilization_ratio": 0.0,
        "gpu_tiler_utilization_ratio": 0.0,
        "gpu_memory_used_mb": 0.0,
        "gpu_memory_total_mb": 0.0,
        "gpu_temperature_c": 0.0,
        "gpu_power_watts": 0.0,
    }
    host_os = detect_host_os_label()
    if host_os == "Mac":
        res["gpu_name"] = "Apple_Silicon_VideoToolbox"
        try:
            out = subprocess.check_output(
                ["/usr/sbin/ioreg", "-r", "-w", "0", "-c", "IOAccelerator"],
                stderr=subprocess.DEVNULL,
                timeout=1.5,
            ).decode("utf-8", errors="replace")
            m_dev = re.search(r'"Device Utilization %"\s*=\s*(\d+)', out)
            m_ren = re.search(r'"Renderer Utilization %"\s*=\s*(\d+)', out)
            m_til = re.search(r'"Tiler Utilization %"\s*=\s*(\d+)', out)
            m_used = re.search(r'"In use system memory"\s*=\s*(\d+)', out)
            m_tot = re.search(r'"Alloc system memory"\s*=\s*(\d+)', out)
            if m_dev:
                res["gpu_core_utilization_ratio"] = min(1.0, float(m_dev.group(1)) / 100.0)
            if m_ren:
                res["gpu_renderer_utilization_ratio"] = min(1.0, float(m_ren.group(1)) / 100.0)
            if m_til:
                res["gpu_tiler_utilization_ratio"] = min(1.0, float(m_til.group(1)) / 100.0)
            mb = 1024.0 * 1024.0
            if m_used:
                res["gpu_memory_used_mb"] = float(m_used.group(1)) / mb
            if m_tot:
                res["gpu_memory_total_mb"] = float(m_tot.group(1)) / mb
        except Exception:
            pass
    elif host_os in ("Linux", "Windows"):
        nvml_libs = ["nvml.dll", r"C:\Windows\System32\nvml.dll"] if host_os == "Windows" else ["libnvidia-ml.so.1", "libnvidia-ml.so"]
        for lib in nvml_libs:
            try:
                nvml = ctypes.CDLL(lib)
                if nvml.nvmlInit_v2() == 0:
                    dev = ctypes.c_void_p()
                    if nvml.nvmlDeviceGetHandleByIndex_v2(ctypes.c_uint(0), ctypes.byref(dev)) == 0:
                        nbuf = ctypes.create_string_buffer(96)
                        if nvml.nvmlDeviceGetName(dev, nbuf, ctypes.c_uint(95)) == 0:
                            res["gpu_name"] = nbuf.value.decode("utf-8", errors="replace").strip()

                        class NvmlUtil(ctypes.Structure):
                            _fields_ = [("gpu", ctypes.c_uint), ("memory", ctypes.c_uint)]
                        u = NvmlUtil()
                        if nvml.nvmlDeviceGetUtilizationRates(dev, ctypes.byref(u)) == 0:
                            res["gpu_core_utilization_ratio"] = min(1.0, u.gpu / 100.0)
                        dec_u, dec_p = ctypes.c_uint(0), ctypes.c_uint(0)
                        if nvml.nvmlDeviceGetDecoderUtilization(dev, ctypes.byref(dec_u), ctypes.byref(dec_p)) == 0:
                            res["gpu_decoder_utilization_ratio"] = min(1.0, dec_u.value / 100.0)

                        class NvmlMem(ctypes.Structure):
                            _fields_ = [("total", ctypes.c_ulonglong), ("free", ctypes.c_ulonglong), ("used", ctypes.c_ulonglong)]
                        mem = NvmlMem()
                        if nvml.nvmlDeviceGetMemoryInfo(dev, ctypes.byref(mem)) == 0:
                            res["gpu_memory_used_mb"] = mem.used / (1024.0 * 1024.0)
                            res["gpu_memory_total_mb"] = mem.total / (1024.0 * 1024.0)
                        tc = ctypes.c_uint(0)
                        if nvml.nvmlDeviceGetTemperature(dev, ctypes.c_uint(0), ctypes.byref(tc)) == 0:
                            res["gpu_temperature_c"] = float(tc.value)
                        pmw = ctypes.c_uint(0)
                        if nvml.nvmlDeviceGetPowerUsage(dev, ctypes.byref(pmw)) == 0:
                            res["gpu_power_watts"] = pmw.value / 1000.0
                    nvml.nvmlShutdown()
                    if res["gpu_name"]:
                        break
            except Exception:
                continue
    return res


# ============================================================================
# 4. Prometheus Text Exposition Merger & HTTP / Push / Textfile Exporter
# ============================================================================

def esc_prom(val: str) -> str:
    return val.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def render_merged_prometheus(
    nodename: str,
    host_os: str,
    proc_sample: Dict[str, Any],
    cpu_ratios: Tuple[float, float, float, float],
    load_avgs: Tuple[float, float, float],
    mem_gb: Tuple[float, float, float, float, float],
    gpu: Dict[str, Any],
    rust_snapshot: Optional[Dict[str, Any]],
) -> str:
    lines: List[str] = []
    node_lbl = f'nodename="{esc_prom(nodename)}",host_os="{esc_prom(host_os)}"'

    # --- Layer 1: Rust Domain Telemetry Snapshot (24-bucket histograms, LRU, SQLite, Funnel, FFprobe) ---
    if rust_snapshot and isinstance(rust_snapshot.get("prometheus_exposition"), str):
        lines.append("# --- Layer 1: rusty_dedup In-Process Domain Metrics ---")
        lines.append(rust_snapshot["prometheus_exposition"].rstrip("\n"))

    # --- Layer 2: OS-Kernel Thread & Child Process Introspection ---
    lines.append("# --- Layer 2: OS-Kernel Thread & Child Process Introspection (External Syscalls) ---")
    lines.append("# HELP rusty_dedup_harness_target_alive Whether the monitored rusty_dedup PID is currently alive (1/0).")
    lines.append("# TYPE rusty_dedup_harness_target_alive gauge")
    lines.append(f"rusty_dedup_harness_target_alive{{{node_lbl},pid=\"{proc_sample.get('pid', 0)}\"}} {1 if proc_sample.get('alive') else 0}")

    threads = proc_sample.get("threads", [])
    if threads:
        lines.append("# HELP rusty_dedup_os_thread_cpu_seconds_total Kernel-measured per-thread user/system CPU seconds.")
        lines.append("# TYPE rusty_dedup_os_thread_cpu_seconds_total counter")
        for t in threads:
            tlbl = f'{node_lbl},thread_name="{esc_prom(t["name"])}",tid="{esc_prom(t["tid"])}"'
            lines.append(f'rusty_dedup_os_thread_cpu_seconds_total{{{tlbl},mode="user"}} {t["user_secs"]:.6f}')
            lines.append(f'rusty_dedup_os_thread_cpu_seconds_total{{{tlbl},mode="system"}} {t["sys_secs"]:.6f}')

        lines.append("# HELP rusty_dedup_os_thread_cpu_usage_ratio Instantaneous kernel-reported thread CPU ratio (0..1).")
        lines.append("# TYPE rusty_dedup_os_thread_cpu_usage_ratio gauge")
        for t in threads:
            tlbl = f'{node_lbl},thread_name="{esc_prom(t["name"])}",tid="{esc_prom(t["tid"])}"'
            lines.append(f'rusty_dedup_os_thread_cpu_usage_ratio{{{tlbl}}} {t["cpu_ratio"]:.4f}')

        lines.append("# HELP rusty_dedup_os_thread_state Kernel thread scheduler state (1=current state).")
        lines.append("# TYPE rusty_dedup_os_thread_state gauge")
        for t in threads:
            tlbl = f'{node_lbl},thread_name="{esc_prom(t["name"])}",tid="{esc_prom(t["tid"])}"'
            lines.append(f'rusty_dedup_os_thread_state{{{tlbl},state="{esc_prom(t["state"])}"}} 1')

    main_p = proc_sample.get("main_proc") or {}
    children = proc_sample.get("children_summary") or {}
    if main_p or children:
        lines.append("# HELP rusty_dedup_os_process_rss_bytes Kernel-reported resident memory by process role.")
        lines.append("# TYPE rusty_dedup_os_process_rss_bytes gauge")
        if main_p:
            lines.append(f'rusty_dedup_os_process_rss_bytes{{{node_lbl},role="rusty_dedup"}} {int(main_p.get("rss_bytes", 0))}')
        for role, agg in sorted(children.items()):
            lines.append(f'rusty_dedup_os_process_rss_bytes{{{node_lbl},role="{esc_prom(role)}"}} {int(agg.get("rss_bytes", 0))}')

        lines.append("# HELP rusty_dedup_os_process_disk_io_bytes_total Kernel-reported cumulative disk I/O bytes by process role.")
        lines.append("# TYPE rusty_dedup_os_process_disk_io_bytes_total counter")
        if main_p:
            lines.append(f'rusty_dedup_os_process_disk_io_bytes_total{{{node_lbl},role="rusty_dedup",direction="read"}} {int(main_p.get("disk_read_bytes", 0))}')
            lines.append(f'rusty_dedup_os_process_disk_io_bytes_total{{{node_lbl},role="rusty_dedup",direction="write"}} {int(main_p.get("disk_write_bytes", 0))}')
        for role, agg in sorted(children.items()):
            lines.append(f'rusty_dedup_os_process_disk_io_bytes_total{{{node_lbl},role="{esc_prom(role)}",direction="read"}} {int(agg.get("disk_read_bytes", 0))}')
            lines.append(f'rusty_dedup_os_process_disk_io_bytes_total{{{node_lbl},role="{esc_prom(role)}",direction="write"}} {int(agg.get("disk_write_bytes", 0))}')

        lines.append("# HELP rusty_dedup_os_child_processes_active Active child subprocesses spawned by rusty_dedup.")
        lines.append("# TYPE rusty_dedup_os_child_processes_active gauge")
        for role in ("ffmpeg", "ffprobe"):
            cnt = int(children.get(role, {}).get("count", 0))
            lines.append(f'rusty_dedup_os_child_processes_active{{{node_lbl},name="{role}"}} {cnt}')

    # --- Layer 3: Host Hardware Telemetry (CPU, LoadAvg, Memory, GPU) ---
    lines.append("# --- Layer 3: Host Hardware Telemetry (OS Syscalls) ---")
    lines.append("# HELP rusty_dedup_host_cpu_utilization_ratio Host CPU utilization ratio by mode.")
    lines.append("# TYPE rusty_dedup_host_cpu_utilization_ratio gauge")
    for mode, val in zip(("user", "system", "idle", "active"), cpu_ratios):
        lines.append(f'rusty_dedup_host_cpu_utilization_ratio{{{node_lbl},mode="{mode}"}} {val:.6f}')

    lines.append("# HELP rusty_dedup_host_load_average Host 1m/5m/15m load average.")
    lines.append("# TYPE rusty_dedup_host_load_average gauge")
    for win, val in zip(("1m", "5m", "15m"), load_avgs):
        lines.append(f'rusty_dedup_host_load_average{{{node_lbl},window="{win}"}} {val:.4f}')

    lines.append("# HELP rusty_dedup_host_memory_gb Host physical memory breakdown in GiB.")
    lines.append("# TYPE rusty_dedup_host_memory_gb gauge")
    for st, val in zip(("used", "free", "total", "wired", "compressed"), mem_gb):
        lines.append(f'rusty_dedup_host_memory_gb{{{node_lbl},state="{st}"}} {val:.4f}')

    if gpu.get("gpu_name"):
        glbl = f'{node_lbl},gpu="{esc_prom(gpu["gpu_name"])}"'
        lines.append("# HELP rusty_dedup_host_gpu_utilization_ratio Host GPU utilization ratio (0..1).")
        lines.append("# TYPE rusty_dedup_host_gpu_utilization_ratio gauge")
        lines.append(f'rusty_dedup_host_gpu_utilization_ratio{{{glbl},engine="core"}} {gpu["gpu_core_utilization_ratio"]:.4f}')
        lines.append(f'rusty_dedup_host_gpu_utilization_ratio{{{glbl},engine="decoder"}} {gpu["gpu_decoder_utilization_ratio"]:.4f}')
        lines.append(f'rusty_dedup_host_gpu_utilization_ratio{{{glbl},engine="renderer"}} {gpu["gpu_renderer_utilization_ratio"]:.4f}')
        lines.append(f'rusty_dedup_host_gpu_utilization_ratio{{{glbl},engine="tiler"}} {gpu["gpu_tiler_utilization_ratio"]:.4f}')
        lines.append("# HELP rusty_dedup_host_gpu_memory_mb Host GPU memory usage in MiB.")
        lines.append("# TYPE rusty_dedup_host_gpu_memory_mb gauge")
        lines.append(f'rusty_dedup_host_gpu_memory_mb{{{glbl},state="used"}} {gpu["gpu_memory_used_mb"]:.2f}')
        lines.append(f'rusty_dedup_host_gpu_memory_mb{{{glbl},state="total"}} {gpu["gpu_memory_total_mb"]:.2f}')

    return "\n".join(lines) + "\n"


def push_prometheus_payload(base_url: str, job_name: str, instance: str, body: str) -> None:
    url = f"{base_url.rstrip('/')}/metrics/job/{job_name}/instance/{instance}"
    req = urllib.request.Request(
        url,
        data=body.encode("utf-8"),
        headers={"Content-Type": "text/plain; version=0.0.4; charset=utf-8"},
        method="PUT",
    )
    with urllib.request.urlopen(req, timeout=5.0) as resp:
        resp.read()


def write_textfile_atomic(path: str, body: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)
    tmp = f"{path}.tmp.{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(body)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


class MetricsState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._payload = "# Waiting for initial telemetry sample...\n"

    def set_payload(self, body: str) -> None:
        with self._lock:
            self._payload = body

    def get_payload(self) -> str:
        with self._lock:
            return self._payload


def start_http_server(listen_addr: str, state: MetricsState) -> http.server.ThreadingHTTPServer:
    host, port_str = listen_addr.rsplit(":", 1)
    port = int(port_str)

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path.startswith("/metrics"):
                body = state.get_payload().encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            elif self.path.startswith("/healthz"):
                body = b"ok\n"
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                body = b"404 Not Found (try /metrics)\n"
                self.send_response(404)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        def log_message(self, fmt: str, *args: Any) -> None:
            pass

    http.server.ThreadingHTTPServer.allow_reuse_address = True
    srv = http.server.ThreadingHTTPServer((host, port), Handler)
    t = threading.Thread(target=srv.serve_forever, name="harness-prom-http", daemon=True)
    t.start()
    return srv


# ============================================================================
# 5. CLI (`argparse` with Detailed `--help`) & Main Harness Orchestration
# ============================================================================

def build_arg_parser() -> argparse.ArgumentParser:
    description = (
        "Companion Prometheus Metrics Harness for rusty_dedup (Zero External Dependencies).\n\n"
        "Launches rusty_dedup as a monitored child process (Harness Mode) or attaches to an\n"
        "already-running PID (Attach Mode). Reads Prometheus settings from `dedup_config.yaml`\n"
        "as the single source of truth, introspects rusty_dedup's OS threads and child\n"
        "ffmpeg/ffprobe processes via native unprivileged C syscalls (macOS libproc, Linux\n"
        "procfs, Windows kernel32), samples host CPU/RAM/GPU hardware, and merges everything\n"
        "with rusty_dedup's atomic domain telemetry snapshot on GET /metrics."
    )
    epilog = (
        "Invocation Examples:\n"
        "  1. Harness Mode (recommended — reads listen_addr & Prometheus settings from dedup_config.yaml):\n"
        "       python3 scripts/rusty_dedup_metrics_harness.py -- \\\n"
        "           ./target/release/rusty_dedup /Volumes/MediaA /Volumes/MediaB -o duplicates.json\n\n"
        "  2. Harness Mode with explicit bind address & 30s post-exit scrape linger:\n"
        "       python3 scripts/rusty_dedup_metrics_harness.py --listen 0.0.0.0:9185 --linger-secs 30 -- \\\n"
        "           ./target/release/rusty_dedup -v /Volumes/MediaA\n\n"
        "  3. Harness Mode using a custom dedup_config.yaml:\n"
        "       python3 scripts/rusty_dedup_metrics_harness.py --config /path/to/dedup_config.yaml -- \\\n"
        "           ./target/release/rusty_dedup -c /path/to/dedup_config.yaml /Volumes/MediaA\n\n"
        "  4. Attach Mode (monitor an already-running rusty_dedup process by PID):\n"
        "       python3 scripts/rusty_dedup_metrics_harness.py --pid 12345 \\\n"
        "           --snapshot /tmp/rusty_dedup_telemetry_12345.json\n\n"
        "  5. Self-Test (verify OS thread syscalls, host hardware samplers & Prometheus rendering):\n"
        "       python3 scripts/rusty_dedup_metrics_harness.py --self-test\n"
    )
    parser = argparse.ArgumentParser(
        prog="rusty_dedup_metrics_harness.py",
        description=description,
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    mode_grp = parser.add_argument_group("Mode Selection")
    mode_grp.add_argument(
        "--pid",
        type=int,
        default=None,
        help="Attach Mode: monitor an already-running rusty_dedup PID instead of spawning a child command.",
    )
    mode_grp.add_argument(
        "--self-test",
        action="store_true",
        help="Verify OS-kernel child thread introspection, host hardware syscalls, and Prometheus exposition, then exit.",
    )
    mode_grp.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="Harness Mode: rusty_dedup command and arguments to launch (separate with `--` before the command).",
    )

    cfg_grp = parser.add_argument_group("Config & Prometheus (defaults loaded from dedup_config.yaml)")
    cfg_grp.add_argument(
        "-c",
        "--config",
        default=None,
        help="Path to dedup_config.yaml (defaults to ./dedup_config.yaml or <repo>/dedup_config.yaml).",
    )
    cfg_grp.add_argument(
        "--listen",
        default=None,
        help="Override Prometheus HTTP scrape bind address (e.g. 0.0.0.0:9185). Pass `none` to disable HTTP server.",
    )
    cfg_grp.add_argument(
        "--pushgateway-url",
        default=None,
        help="Override local HTTP Prometheus Pushgateway URL (e.g. http://127.0.0.1:9091).",
    )
    cfg_grp.add_argument(
        "--push-interval-secs",
        type=int,
        default=None,
        help="Override interval in seconds between Pushgateway pushes.",
    )
    cfg_grp.add_argument(
        "--job-name",
        default=None,
        help="Override Prometheus Pushgateway job label (default from dedup_config.yaml: rusty_dedup).",
    )
    cfg_grp.add_argument(
        "--textfile-path",
        default=None,
        help="Override atomic `.prom` textfile collector output path.",
    )
    cfg_grp.add_argument(
        "--nodename",
        default=socket.gethostname() or "local-host",
        help="Override `nodename` Prometheus label for Grafana parity with rusty_h265_converter (default: hostname).",
    )

    life_grp = parser.add_argument_group("Sampling & Lifecycle")
    life_grp.add_argument(
        "--snapshot",
        default=None,
        help="Path to rusty_dedup atomic JSON telemetry snapshot (auto-assigned /tmp/rusty_dedup_telemetry_<pid>.json in Harness Mode).",
    )
    life_grp.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        help="Polling cadence in seconds for OS thread syscalls, host hardware, and snapshot reads (default: 1.0).",
    )
    life_grp.add_argument(
        "--linger-secs",
        type=float,
        default=20.0,
        help="Seconds to keep GET /metrics alive after rusty_dedup exits so Prometheus scrapes final counters (default: 20.0).",
    )
    life_grp.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print diagnostic startup, thread count, and shutdown drain logs to stderr.",
    )
    return parser


def read_snapshot_json(path: Optional[str]) -> Optional[Dict[str, Any]]:
    if not path or not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return json.load(f)
    except Exception:
        return None


def run_self_test(nodename: str) -> int:
    host_os = detect_host_os_label()
    introspector = ProcessThreadIntrospector()
    cpu_tracker = CpuTickTracker()

    # Spawn a short-lived multithreaded child process to verify cross-PID OS thread introspection
    child_code = (
        "import threading, time\n"
        "def w(): time.sleep(1.5)\n"
        "ts = [threading.Thread(target=w, name=f'worker-{i}') for i in range(2)]\n"
        "[t.start() for t in ts]\n"
        "time.sleep(1.5)\n"
    )
    child = subprocess.Popen([sys.executable, "-c", child_code])
    try:
        time.sleep(0.2)
        proc_sample = introspector.sample_target_process(child.pid)
        cpu_ratios = cpu_tracker.sample_cpu_ratios()
        load_avgs = get_load_averages_syscall()
        mem_gb = get_memory_stats_gb_syscall()
        gpu = sample_gpu_stats_syscall()
    finally:
        child.terminate()
        try:
            child.wait(timeout=2.0)
        except Exception:
            child.kill()

    body = render_merged_prometheus(
        nodename=nodename,
        host_os=host_os,
        proc_sample=proc_sample,
        cpu_ratios=cpu_ratios,
        load_avgs=load_avgs,
        mem_gb=mem_gb,
        gpu=gpu,
        rust_snapshot=None,
    )
    n_threads = len(proc_sample.get("threads", []))
    print(f"[Self-Test] host_os={host_os}, nodename={nodename}, child_alive={proc_sample['alive']}, child_threads_introspected={n_threads}")
    print(f"[Self-Test] host_cpu_active={cpu_ratios[3]*100:.1f}%, mem_used={mem_gb[0]:.2f}/{mem_gb[2]:.2f} GiB, gpu={gpu.get('gpu_name') or 'none'}")
    print("[Self-Test] Sample Prometheus v0.0.4 lines:")
    for line in body.splitlines()[:25]:
        print("  " + line)
    if not proc_sample["alive"] or n_threads == 0:
        print("[Self-Test] ERROR: Failed to introspect child process threads!", file=sys.stderr)
        return 1
    print("[Self-Test] PASS")
    return 0


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    if args.self_test:
        return run_self_test(args.nodename)

    cmd = list(args.command)
    if cmd and cmd[0] == "--":
        cmd = cmd[1:]

    if not cmd and args.pid is None:
        parser.error("Provide either `-- <rusty_dedup command ...>` (Harness Mode) or `--pid <PID>` (Attach Mode). Run with --help for examples.")

    cfg_path = resolve_default_config_path(args.config)
    cfg = load_dedup_config_yaml(cfg_path)

    listen_addr = cfg["listen_addr"]
    if args.listen is not None:
        listen_addr = _parse_optional_str(args.listen)
    pushgateway_url = _parse_optional_str(args.pushgateway_url) if args.pushgateway_url is not None else cfg["pushgateway_url"]
    push_interval = args.push_interval_secs if args.push_interval_secs is not None else cfg["push_interval_secs"]
    job_name = args.job_name or cfg["job_name"]
    textfile_path = _parse_optional_str(args.textfile_path) if args.textfile_path is not None else cfg["textfile_path"]

    host_os = detect_host_os_label()
    state = MetricsState()
    http_srv: Optional[http.server.ThreadingHTTPServer] = None

    if listen_addr:
        try:
            http_srv = start_http_server(listen_addr, state)
            print(
                f"[Harness] Prometheus HTTP endpoint serving http://{listen_addr}/metrics "
                f"(config={cfg_path or 'defaults'}, nodename={args.nodename}, host_os={host_os})",
                file=sys.stderr,
            )
        except OSError as e:
            print(f"[Harness] Warning: Failed to bind {listen_addr}: {e}", file=sys.stderr)

    child_proc: Optional[subprocess.Popen] = None
    snapshot_path = args.snapshot or cfg["snapshot_path"]
    created_temp_snapshot = False

    if cmd:
        if not snapshot_path:
            snapshot_path = f"/tmp/rusty_dedup_telemetry_{os.getpid()}.json"
            created_temp_snapshot = True
        env = os.environ.copy()
        env["RUSTY_DEDUP_HARNESS_ACTIVE"] = "1"
        env["RUSTY_DEDUP_TELEMETRY_SNAPSHOT"] = snapshot_path
        if args.verbose:
            print(f"[Harness] Launching child: {cmd} (snapshot={snapshot_path})", file=sys.stderr)
        child_proc = subprocess.Popen(cmd, env=env)
        target_pid = child_proc.pid

        def _forward_sig(signum: int, _frame: Any) -> None:
            if child_proc and child_proc.poll() is None:
                try:
                    child_proc.send_signal(signum)
                except Exception:
                    pass

        signal.signal(signal.SIGINT, _forward_sig)
        signal.signal(signal.SIGTERM, _forward_sig)
    else:
        target_pid = int(args.pid)
        if args.verbose:
            print(f"[Harness] Attaching to existing PID={target_pid} (snapshot={snapshot_path})", file=sys.stderr)

    introspector = ProcessThreadIntrospector()
    cpu_tracker = CpuTickTracker()
    last_push = 0.0
    poll_interval = max(0.2, float(args.poll_interval))
    exit_code = 0

    def _poll_and_publish(force_export: bool = False) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
        nonlocal last_push
        proc_sample = introspector.sample_target_process(target_pid)
        cpu_ratios = cpu_tracker.sample_cpu_ratios()
        load_avgs = get_load_averages_syscall()
        mem_gb = get_memory_stats_gb_syscall()
        gpu = sample_gpu_stats_syscall()
        snap = read_snapshot_json(snapshot_path)

        body = render_merged_prometheus(
            nodename=args.nodename,
            host_os=host_os,
            proc_sample=proc_sample,
            cpu_ratios=cpu_ratios,
            load_avgs=load_avgs,
            mem_gb=mem_gb,
            gpu=gpu,
            rust_snapshot=snap,
        )
        state.set_payload(body)

        now = time.monotonic()
        if force_export or (now - last_push >= push_interval):
            if pushgateway_url:
                instance = (snap or {}).get("summary", {}).get("meta", {}).get("session_id") or args.nodename
                try:
                    push_prometheus_payload(pushgateway_url, job_name, str(instance), body)
                except Exception as e:
                    if args.verbose:
                        print(f"[Harness] Warning: Pushgateway push failed: {e}", file=sys.stderr)
            if textfile_path:
                try:
                    write_textfile_atomic(textfile_path, body)
                except Exception as e:
                    if args.verbose:
                        print(f"[Harness] Warning: Textfile write failed: {e}", file=sys.stderr)
            last_push = now
        return proc_sample, snap

    try:
        while True:
            proc_sample, snap = _poll_and_publish(force_export=False)
            if child_proc is not None:
                rc = child_proc.poll()
                if rc is not None:
                    exit_code = int(rc)
                    break
            else:
                if not proc_sample.get("alive", False):
                    break
            time.sleep(poll_interval)

        # Final drain sample + push/textfile write after child finishes
        _poll_and_publish(force_export=True)
        if http_srv is not None and args.linger_secs > 0:
            print(
                f"[Harness] Target process exited (code={exit_code}). Holding http://{listen_addr}/metrics "
                f"for {args.linger_secs:.0f}s for final Prometheus scrape...",
                file=sys.stderr,
            )
            deadline = time.monotonic() + float(args.linger_secs)
            while time.monotonic() < deadline:
                time.sleep(0.5)
    except KeyboardInterrupt:
        exit_code = 130
    finally:
        if http_srv is not None:
            try:
                http_srv.shutdown()
                http_srv.server_close()
            except Exception:
                pass
        if created_temp_snapshot and snapshot_path and os.path.exists(snapshot_path):
            try:
                os.remove(snapshot_path)
            except Exception:
                pass

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
