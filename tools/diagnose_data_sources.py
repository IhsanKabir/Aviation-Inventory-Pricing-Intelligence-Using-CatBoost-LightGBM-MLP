"""
Data Source Connectivity Diagnostic Tool

This tool diagnoses connectivity issues with all external data sources:
- ShareTrip (api.sharetrip.net)
- BDFare (bdfare.com)
- GOzyaan (production.gozayaan.com)
- AMYBD (www.amybd.com)
- TTInteractive (requires browser-based access)

Usage:
    python tools/diagnose_data_sources.py
    python tools/diagnose_data_sources.py --verbose
    python tools/diagnose_data_sources.py --check sharetrip
    python tools/diagnose_data_sources.py --output-json output/reports/connectivity_check.json
"""

import argparse
import json
import logging
import os
import socket
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    print("ERROR: requests library not installed. Install with: pip install requests")
    sys.exit(1)


LOG = logging.getLogger(__name__)


# Data source configurations
DATA_SOURCES = {
    "sharetrip": {
        "name": "ShareTrip",
        "base_url": "https://api.sharetrip.net",
        "test_endpoint": "/api/v2/flight/search/initialize",
        "method": "GET",
        "required_for": ["BS", "2A", "SV", "G9", "3L", "FZ", "EK", "QR", "WY", "CZ", "8D", "UL", "MH", "AK", "OD", "SQ", "TG", "6E"],
        "env_vars": ["SHARETRIP_ACCESS_TOKEN", "SHARETRIP_API_BASE"],
    },
    "bdfare": {
        "name": "BDFare",
        "base_url": "https://bdfare.com",
        "test_endpoint": "/bdfare-search/api/v2/Search/AirSearch",
        "method": "POST",
        "required_for": ["BS", "2A", "BG", "VQ"],
        "env_vars": ["BDFARE_API_BASE", "BDFARE_COOKIES_PATH"],
    },
    "gozayaan": {
        "name": "GOzyaan",
        "base_url": "https://production.gozayaan.com",
        "test_endpoint": "/api/flight/v4.0/search/",
        "method": "POST",
        "required_for": ["BS", "2A"],
        "env_vars": ["GOZAYAAN_X_KONG_SEGMENT_ID", "GOZAYAAN_API_BASE"],
    },
    "amybd": {
        "name": "AMYBD",
        "base_url": "https://www.amybd.com",
        "test_endpoint": "/atapi.aspx",
        "method": "POST",
        "required_for": ["BS", "2A"],
        "env_vars": ["AMYBD_TOKEN", "AMYBD_API_URL"],
    },
}


def check_dns_resolution(hostname: str) -> Dict[str, Any]:
    """Check if a hostname can be resolved to an IP address."""
    result = {
        "hostname": hostname,
        "resolvable": False,
        "ip_addresses": [],
        "error": None,
    }

    try:
        ip_addresses = socket.getaddrinfo(hostname, None)
        unique_ips = list(set([addr[4][0] for addr in ip_addresses]))
        result["resolvable"] = True
        result["ip_addresses"] = unique_ips
        LOG.info(f"DNS resolution for {hostname}: {unique_ips}")
    except socket.gaierror as e:
        result["error"] = f"DNS resolution failed: {str(e)}"
        LOG.error(f"DNS resolution failed for {hostname}: {e}")
    except Exception as e:
        result["error"] = f"Unexpected error: {str(e)}"
        LOG.error(f"Unexpected DNS error for {hostname}: {e}")

    return result


def check_tcp_connectivity(hostname: str, port: int = 443, timeout: float = 5.0) -> Dict[str, Any]:
    """Check if a TCP connection can be established to a host."""
    result = {
        "hostname": hostname,
        "port": port,
        "connectable": False,
        "response_time_ms": None,
        "error": None,
    }

    try:
        start_time = time.time()
        sock = socket.create_connection((hostname, port), timeout=timeout)
        sock.close()
        response_time = (time.time() - start_time) * 1000
        result["connectable"] = True
        result["response_time_ms"] = round(response_time, 2)
        LOG.info(f"TCP connection to {hostname}:{port} successful ({response_time:.2f}ms)")
    except socket.timeout:
        result["error"] = f"Connection timeout after {timeout}s"
        LOG.error(f"TCP connection timeout to {hostname}:{port}")
    except socket.error as e:
        result["error"] = f"Connection error: {str(e)}"
        LOG.error(f"TCP connection error to {hostname}:{port}: {e}")
    except Exception as e:
        result["error"] = f"Unexpected error: {str(e)}"
        LOG.error(f"Unexpected TCP error for {hostname}:{port}: {e}")

    return result


def check_http_connectivity(
    source_key: str,
    config: Dict[str, Any],
    timeout: float = 10.0,
    proxy_url: Optional[str] = None
) -> Dict[str, Any]:
    """Check if HTTP/HTTPS connectivity works for a data source."""
    result = {
        "source": source_key,
        "name": config["name"],
        "url": config["base_url"] + config["test_endpoint"],
        "method": config["method"],
        "accessible": False,
        "status_code": None,
        "response_time_ms": None,
        "error": None,
        "proxy_used": proxy_url,
    }

    # Create session with retry logic
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "POST", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    if proxy_url:
        session.proxies.update({"http": proxy_url, "https": proxy_url})

    # Set headers
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json, text/plain, */*",
    }

    try:
        start_time = time.time()
        if config["method"] == "GET":
            response = session.get(
                result["url"],
                headers=headers,
                timeout=timeout,
                allow_redirects=True
            )
        else:  # POST
            response = session.post(
                result["url"],
                headers=headers,
                json={},
                timeout=timeout,
                allow_redirects=True
            )

        response_time = (time.time() - start_time) * 1000
        result["accessible"] = True
        result["status_code"] = response.status_code
        result["response_time_ms"] = round(response_time, 2)

        LOG.info(
            f"HTTP {config['method']} to {config['name']} successful: "
            f"status={response.status_code}, time={response_time:.2f}ms"
        )

    except requests.exceptions.ConnectionError as e:
        result["error"] = f"Connection error: {str(e)}"
        LOG.error(f"HTTP connection error for {config['name']}: {e}")
    except requests.exceptions.Timeout:
        result["error"] = f"Request timeout after {timeout}s"
        LOG.error(f"HTTP timeout for {config['name']}")
    except requests.exceptions.RequestException as e:
        result["error"] = f"Request error: {str(e)}"
        LOG.error(f"HTTP request error for {config['name']}: {e}")
    except Exception as e:
        result["error"] = f"Unexpected error: {str(e)}"
        LOG.error(f"Unexpected HTTP error for {config['name']}: {e}")

    return result


def check_environment_variables(config: Dict[str, Any]) -> Dict[str, Any]:
    """Check if required environment variables are set."""
    result = {
        "env_vars": {},
        "all_set": True,
    }

    for var_name in config.get("env_vars", []):
        value = os.getenv(var_name)
        is_set = value is not None and value.strip() != ""
        result["env_vars"][var_name] = {
            "set": is_set,
            "value_preview": (value[:20] + "..." if value and len(value) > 20 else value) if is_set else None
        }
        if not is_set:
            result["all_set"] = False

    return result


def diagnose_source(
    source_key: str,
    config: Dict[str, Any],
    proxy_url: Optional[str] = None,
    skip_http: bool = False
) -> Dict[str, Any]:
    """Run comprehensive diagnostics for a data source."""
    LOG.info(f"Diagnosing {config['name']} ({source_key})...")

    parsed_url = urlparse(config["base_url"])
    hostname = parsed_url.hostname or ""
    port = parsed_url.port or (443 if parsed_url.scheme == "https" else 80)

    result = {
        "source": source_key,
        "name": config["name"],
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "dns": check_dns_resolution(hostname),
        "tcp": check_tcp_connectivity(hostname, port),
        "env": check_environment_variables(config),
        "http": None,
        "overall_status": "FAIL",
        "issues": [],
        "recommendations": [],
    }

    # HTTP check (may fail in restricted environments)
    if not skip_http:
        result["http"] = check_http_connectivity(source_key, config, proxy_url=proxy_url)

    # Determine overall status and issues
    if not result["dns"]["resolvable"]:
        result["issues"].append(f"DNS resolution failed for {hostname}")
        result["recommendations"].append(
            f"Check network connectivity or use a DNS server that can resolve {hostname}"
        )

    if not result["tcp"]["connectable"]:
        result["issues"].append(f"Cannot establish TCP connection to {hostname}:{port}")
        result["recommendations"].append(
            f"Check if {hostname} is accessible from your network. "
            "You may be behind a firewall or in a restricted environment."
        )

    if result["http"] and not result["http"]["accessible"]:
        result["issues"].append(f"HTTP {config['method']} request failed")
        result["recommendations"].append(
            "Consider using a proxy server or VPN if you're in a restricted network environment."
        )

    if not result["env"]["all_set"]:
        missing_vars = [
            var for var, info in result["env"]["env_vars"].items()
            if not info["set"]
        ]
        result["issues"].append(f"Missing environment variables: {', '.join(missing_vars)}")
        result["recommendations"].append(
            f"Set required environment variables: {', '.join(missing_vars)}"
        )

    # Determine overall status
    if result["dns"]["resolvable"] and result["tcp"]["connectable"]:
        if result["http"] and result["http"]["accessible"]:
            result["overall_status"] = "PASS"
        elif result["http"]:
            result["overall_status"] = "WARN"  # TCP works but HTTP fails
        else:
            result["overall_status"] = "PASS"  # Skip HTTP check
    else:
        result["overall_status"] = "FAIL"

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Diagnose connectivity to aviation data sources"
    )
    parser.add_argument(
        "--check",
        choices=list(DATA_SOURCES.keys()) + ["all"],
        default="all",
        help="Which data source to check (default: all)"
    )
    parser.add_argument(
        "--proxy",
        help="Proxy URL to use for HTTP checks (e.g., http://proxy.example.com:8080)"
    )
    parser.add_argument(
        "--skip-http",
        action="store_true",
        help="Skip HTTP connectivity checks (useful in restricted environments)"
    )
    parser.add_argument(
        "--output-json",
        help="Save results to a JSON file"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    # Determine which sources to check
    if args.check == "all":
        sources_to_check = DATA_SOURCES.items()
    else:
        sources_to_check = [(args.check, DATA_SOURCES[args.check])]

    # Run diagnostics
    results = []
    for source_key, config in sources_to_check:
        result = diagnose_source(
            source_key,
            config,
            proxy_url=args.proxy,
            skip_http=args.skip_http
        )
        results.append(result)

        # Print summary
        status_symbol = "✓" if result["overall_status"] == "PASS" else "✗"
        print(f"\n{status_symbol} {result['name']} ({source_key}): {result['overall_status']}")

        if result["issues"]:
            print("  Issues:")
            for issue in result["issues"]:
                print(f"    - {issue}")

        if result["recommendations"]:
            print("  Recommendations:")
            for rec in result["recommendations"]:
                print(f"    - {rec}")

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    pass_count = sum(1 for r in results if r["overall_status"] == "PASS")
    warn_count = sum(1 for r in results if r["overall_status"] == "WARN")
    fail_count = sum(1 for r in results if r["overall_status"] == "FAIL")

    print(f"Total sources checked: {len(results)}")
    print(f"  PASS: {pass_count}")
    print(f"  WARN: {warn_count}")
    print(f"  FAIL: {fail_count}")

    # Save to JSON if requested
    if args.output_json:
        output_path = Path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        output_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "summary": {
                "total": len(results),
                "pass": pass_count,
                "warn": warn_count,
                "fail": fail_count,
            },
            "results": results,
        }

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)

        print(f"\nResults saved to: {output_path}")

    # Exit with appropriate code
    sys.exit(0 if fail_count == 0 else 1)


if __name__ == "__main__":
    main()
