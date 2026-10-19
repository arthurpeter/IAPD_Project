
import os
import random
import re
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(ROOT_DIR / ".env")

TEST_MODE = False
TEST_DATE = "2025-01-01"
START_DATE = "2016-01-03"
END_DATE = "2025-12-31"

MAX_RETRIES = 5
REQUEST_DELAY = 1
TIMEOUT = (15, 90)
MAX_BACKOFF = 1800

API_BACKOFF_BASE = {
    "countries": 10,
    "geocoding": 15,
    "openmeteo": 60,
    "nasa": 30,
    "meteostat": 30,
}

METEOSTAT_API_KEY = os.getenv("METEOSTAT_API_KEY")

DATA_DIR = ROOT_DIR / "data"
CAPITALS_FILE = DATA_DIR / "capitals.csv"
DATASET_FILE = DATA_DIR / (
    "weather_test.csv" if TEST_MODE else "weather_raw.csv"
)

DATA_SOURCES = {
    "countries": "https://countriesnow.space/api/v0.1/countries/capital",
    "geocoding": "https://geocoding-api.open-meteo.com/v1/search",
    "openmeteo": "https://archive-api.open-meteo.com/v1/archive",
    "nasa": "https://power.larc.nasa.gov/api/temporal/daily/point",
    "meteostat": "https://meteostat.p.rapidapi.com/point/daily",
}

DAILY_VARIABLES = [
    "weather_code",
    "temperature_2m_mean",
    "temperature_2m_max",
    "temperature_2m_min",
    "apparent_temperature_mean",
    "apparent_temperature_max",
    "apparent_temperature_min",
    "sunrise",
    "sunset",
    "daylight_duration",
    "sunshine_duration",
    "precipitation_sum",
    "rain_sum",
    "snowfall_sum",
    "precipitation_hours",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
    "wind_direction_10m_dominant",
    "shortwave_radiation_sum",
    "et0_fao_evapotranspiration",
    "cloud_cover_mean",
    "dew_point_2m_mean",
    "dew_point_2m_max",
    "dew_point_2m_min",
    "relative_humidity_2m_mean",
    "relative_humidity_2m_max",
    "relative_humidity_2m_min",
    "pressure_msl_mean",
    "wind_speed_10m_mean",
    "wet_bulb_temperature_2m_mean",
    "vapour_pressure_deficit_max",
    "soil_moisture_0_to_7cm_mean",
    "soil_moisture_7_to_28cm_mean",
    "soil_moisture_28_to_100cm_mean",
    "soil_moisture_0_to_100cm_mean",
    "soil_temperature_0_to_7cm_mean",
    "soil_temperature_7_to_28cm_mean",
    "soil_temperature_28_to_100cm_mean",
]

HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "apparent_temperature",
    "precipitation",
    "rain",
    "snowfall",
    "snow_depth",
    "weather_code",
    "pressure_msl",
    "surface_pressure",
    "cloud_cover",
    "cloud_cover_low",
    "cloud_cover_mid",
    "cloud_cover_high",
    "et0_fao_evapotranspiration",
    "vapour_pressure_deficit",
    "wind_speed_10m",
    "wind_speed_100m",
    "wind_direction_10m",
    "wind_direction_100m",
    "wind_gusts_10m",
    "soil_temperature_0_to_7cm",
    "soil_temperature_7_to_28cm",
    "soil_temperature_28_to_100cm",
    "soil_temperature_100_to_255cm",
    "soil_moisture_0_to_7cm",
    "soil_moisture_7_to_28cm",
    "soil_moisture_28_to_100cm",
    "soil_moisture_100_to_255cm",
    "soil_moisture_0_to_100cm",
    "soil_temperature_0_to_100cm",
    "soil_moisture_index_0_to_7cm",
    "soil_moisture_index_7_to_28cm",
    "soil_moisture_index_28_to_100cm",
    "soil_moisture_index_0_to_100cm",
    "boundary_layer_height",
    "wet_bulb_temperature_2m",
    "total_column_integrated_water_vapour",
    "is_day",
    "sunshine_duration",
    "growing_degree_days_base_0_limit_50",
]

SUM_HOURLY_VARIABLES = {
    "precipitation",
    "rain",
    "snowfall",
    "sunshine_duration",
    "et0_fao_evapotranspiration",
}

NASA_GROUPS = {
    "sb": [
        "T2M",
        "T2M_MAX",
        "T2M_MIN",
        "T2MDEW",
        "T2MWET",
        "T2M_RANGE",
        "TS",
        "RH2M",
        "PS",
        "WS10M",
        "WS10M_MAX",
        "WS10M_MIN",
        "WD10M",
        "WS50M",
        "WS50M_MAX",
        "WS50M_MIN",
        "WD50M",
        "PRECTOTCORR",
    ],
    "re": [
        "ALLSKY_SFC_SW_DWN",
        "CLRSKY_SFC_SW_DWN",
        "ALLSKY_SFC_LW_DWN",
        "CLRSKY_SFC_LW_DWN",
        "ALLSKY_SFC_PAR_TOT",
        "ALLSKY_SFC_UV_INDEX",
    ],
}

session = requests.Session()
session.headers.update({"User-Agent": "IAPD-Weather-Project/1.0"})

RATE_LIMIT_STATE = {
    name: {
        "next_wait": base_wait,
        "cooldown_until": 0.0,
        "total_429": 0,
    }
    for name, base_wait in API_BACKOFF_BASE.items()
}


def get_json(url, params=None, headers=None):
    api_name = next(
        (
            name
            for name, endpoint in DATA_SOURCES.items()
            if endpoint == url
        ),
        None,
    )

    if api_name is None:
        raise ValueError(f"Unknown API endpoint: {url}")

    state = RATE_LIMIT_STATE[api_name]

    for attempt in range(MAX_RETRIES + 1):
        remaining_cooldown = state["cooldown_until"] - time.monotonic()

        if remaining_cooldown > 0:
            print(
                f"{api_name}: waiting {remaining_cooldown:.1f}s "
                "for rate-limit cooldown"
            )
            time.sleep(remaining_cooldown)

        try:
            response = session.get(
                url,
                params=params,
                headers=headers,
                timeout=TIMEOUT,
            )

            if response.status_code == 429:
                state["total_429"] += 1

                retry_after = response.headers.get("Retry-After")

                try:
                    server_wait = max(0.0, float(retry_after))
                except (TypeError, ValueError):
                    server_wait = 0.0

                wait = max(state["next_wait"], server_wait)

                state["next_wait"] = min(
                    max(wait * 2, API_BACKOFF_BASE[api_name]),
                    MAX_BACKOFF,
                )

                state["cooldown_until"] = time.monotonic() + wait

                print(
                    f"{api_name}: HTTP 429 "
                    f"(total: {state['total_429']}); "
                    f"cooldown {wait:.1f}s; "
                    f"next backoff {state['next_wait']}s"
                )

                if attempt == MAX_RETRIES:
                    response.raise_for_status()

                time.sleep(
                    wait + random.uniform(0, min(5, wait * 0.1))
                )
                continue

            if response.status_code in {500, 502, 503, 504}:
                if attempt == MAX_RETRIES:
                    response.raise_for_status()

                wait = min(
                    API_BACKOFF_BASE[api_name] * (2 ** attempt),
                    MAX_BACKOFF,
                )
                wait += random.uniform(0, min(5, wait * 0.1))

                print(
                    f"{api_name}: HTTP {response.status_code}; "
                    f"retrying in {wait:.1f}s"
                )
                time.sleep(wait)
                continue

            response.raise_for_status()
            data = response.json()

            if isinstance(data, dict) and data.get("error"):
                raise RuntimeError(
                    data.get("reason")
                    or data.get("message")
                    or "API returned an error"
                )

            time.sleep(REQUEST_DELAY)
            return data

        except (requests.Timeout, requests.ConnectionError):
            if attempt == MAX_RETRIES:
                raise

            wait = min(
                API_BACKOFF_BASE[api_name] * (2 ** attempt),
                MAX_BACKOFF,
            )
            wait += random.uniform(0, min(5, wait * 0.1))

            print(
                f"{api_name}: connection error; "
                f"retrying in {wait:.1f}s"
            )
            time.sleep(wait)

    raise RuntimeError(f"{api_name}: request failed after retries")


def load_capitals():
    if CAPITALS_FILE.exists():
        capitals = pd.read_csv(CAPITALS_FILE)

        for column in (
            "capital",
            "country",
            "country_code",
            "latitude",
            "longitude",
            "elevation",
        ):
            if column not in capitals.columns:
                capitals[column] = float("nan")

        return capitals

    response = get_json(DATA_SOURCES["countries"])
    countries = response.get("data", [])
    rows = []

    for item in countries:
        capital = item.get("capital")
        name = item.get("name")
        code = item.get("iso2")

        if not isinstance(capital, str) or not capital.strip():
            continue

        if not name or not code:
            continue

        rows.append({
            "capital": capital.strip(),
            "country": name,
            "country_code": str(code).upper(),
            "latitude": float("nan"),
            "longitude": float("nan"),
            "elevation": float("nan"),
        })

    capitals = pd.DataFrame(rows)
    capitals = capitals.drop_duplicates(
        subset=["country_code", "capital"]
    )
    capitals = capitals.sort_values("country").reset_index(drop=True)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    capitals.to_csv(CAPITALS_FILE, index=False)

    print(f"Saved {len(capitals)} capitals")
    return capitals


def fill_coordinates(capitals):
    capitals = capitals.copy()

    for index in capitals.index:
        latitude = capitals.loc[index, "latitude"]
        longitude = capitals.loc[index, "longitude"]

        if pd.notna(latitude) and pd.notna(longitude):
            continue

        capital = capitals.loc[index, "capital"]
        code = str(capitals.loc[index, "country_code"]).upper()

        params = {
            "name": capital,
            "countryCode": code,
            "count": 10,
            "language": "en",
            "format": "json",
        }

        try:
            response = get_json(DATA_SOURCES["geocoding"], params)
            results = response.get("results", [])

            match = next(
                (
                    item for item in results
                    if str(item.get("country_code", "")).upper() == code
                ),
                results[0] if results else None,
            )

            if match:
                capitals.loc[index, "latitude"] = match.get("latitude")
                capitals.loc[index, "longitude"] = match.get("longitude")
                capitals.loc[index, "elevation"] = match.get("elevation")
            else:
                print(f"No coordinates found for {capital}, {code}")

        except (requests.RequestException, ValueError, RuntimeError) as exc:
            print(f"Geocoding failed for {capital}: {exc}")

        capitals.to_csv(CAPITALS_FILE, index=False)

    return capitals


def fetch_openmeteo(location, start_date, end_date):
    params = {
        "latitude": location.latitude,
        "longitude": location.longitude,
        "start_date": start_date,
        "end_date": end_date,
        "daily": ",".join(DAILY_VARIABLES),
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": "auto",
        "models": "era5",
    }

    response = get_json(DATA_SOURCES["openmeteo"], params)
    result = response[0] if isinstance(response, list) else response
    frames = []

    daily = result.get("daily", {})

    if daily.get("time"):
        frame = pd.DataFrame(daily)
        frame["date"] = pd.to_datetime(frame.pop("time")).dt.strftime(
            "%Y-%m-%d"
        )
        frame = frame.rename(columns={
            column: f"om_daily_{column}"
            for column in frame.columns
            if column != "date"
        })
        frames.append(frame)

    hourly = result.get("hourly", {})

    if hourly.get("time"):
        hours = pd.DataFrame(hourly)
        hours["date"] = pd.to_datetime(hours.pop("time")).dt.strftime(
            "%Y-%m-%d"
        )

        aggregations = {}

        for column in hours.columns:
            if column == "date":
                continue

            aggregations[column] = ["mean", "min", "max"]

            if column in SUM_HOURLY_VARIABLES:
                aggregations[column].append("sum")

        aggregated = hours.groupby("date").agg(aggregations)
        aggregated.columns = [
            f"om_hourly_{variable}_{stat}"
            for variable, stat in aggregated.columns
        ]
        frames.append(aggregated.reset_index())

    if not frames:
        raise RuntimeError("Open-Meteo returned no daily or hourly data")

    frame = frames[0]

    for other in frames[1:]:
        frame = frame.merge(other, on="date", how="outer")

    return frame


def nasa_request(location, start_date, end_date, group, parameters):
    remaining = list(parameters)
    rejected = set()

    while remaining:
        params = {
            "parameters": ",".join(remaining),
            "community": group.upper(),
            "longitude": location.longitude,
            "latitude": location.latitude,
            "start": start_date.replace("-", ""),
            "end": end_date.replace("-", ""),
            "format": "JSON",
            "time-standard": "UTC",
        }

        try:
            response = get_json(DATA_SOURCES["nasa"], params)
            return response, sorted(rejected)

        except requests.HTTPError as exc:
            response = exc.response

            if response is None or response.status_code != 422:
                raise

            body = response.json()
            messages = body.get("messages", [])
            message = " ".join(str(item) for item in messages)

            invalid = [
                parameter for parameter in remaining
                if re.search(
                    rf"\b{re.escape(parameter)}\b",
                    message,
                    flags=re.IGNORECASE,
                )
            ]

            if not invalid:
                raise RuntimeError(
                    f"NASA rejected the request: {message}"
                ) from exc

            for parameter in invalid:
                remaining.remove(parameter)
                rejected.add(parameter)
                print(
                    f"NASA rejected {parameter} for {group.upper()}; "
                    "removing it and retrying"
                )

    return None, sorted(rejected)


def fetch_nasa_group(location, start_date, end_date, group, parameters):
    response, rejected = nasa_request(
        location,
        start_date,
        end_date,
        group,
        parameters,
    )

    if response is None:
        return None

    values = response.get("properties", {}).get("parameter", {})
    frames = []

    for parameter, date_values in values.items():
        series = pd.Series(
            date_values,
            name=f"nasa_{group}_{parameter}",
            dtype="float64",
        )
        series.index = pd.to_datetime(
            series.index.astype(str),
            format="%Y%m%d",
            errors="coerce",
        )
        series.index.name = "date"
        frames.append(series)

    if not frames:
        return None

    frame = pd.concat(frames, axis=1).reset_index()
    frame["date"] = pd.to_datetime(frame["date"]).dt.strftime("%Y-%m-%d")
    frame = frame.replace(-999, float("nan"))

    if rejected:
        print(
            f"NASA {group.upper()} unavailable parameters: "
            f"{', '.join(rejected)}"
        )

    return frame


def fetch_meteostat(location, start_date, end_date):
    if not METEOSTAT_API_KEY:
        return None

    params = {
        "lat": location.latitude,
        "lon": location.longitude,
        "start": start_date,
        "end": end_date,
    }

    response = get_json(
        DATA_SOURCES["meteostat"],
        params,
        headers={
            "x-rapidapi-key": METEOSTAT_API_KEY,
            "x-rapidapi-host": "meteostat.p.rapidapi.com",
        },
    )

    observations = response.get("data", [])

    if not observations:
        return None

    frame = pd.DataFrame(observations)

    if "date" not in frame.columns:
        return None

    frame["date"] = pd.to_datetime(frame["date"]).dt.strftime("%Y-%m-%d")
    frame = frame.rename(columns={
        column: f"meteostat_{column}"
        for column in frame.columns
        if column != "date"
    })

    return frame


def fetch_location(location, start_date, end_date):
    dates = pd.date_range(start_date, end_date, freq="D")

    frame = pd.DataFrame({
        "date": dates.strftime("%Y-%m-%d"),
    })

    failed_sources = []

    def merge_source(data):
        nonlocal frame

        if data is not None:
            frame = frame.merge(data, on="date", how="left")

    try:
        data = fetch_openmeteo(location, start_date, end_date)
        merge_source(data)
        print(f"openmeteo: {location.capital}: OK")
    except Exception as exc:
        failed_sources.append("openmeteo")
        print(f"openmeteo: {location.capital}: {exc}")

    for group, parameters in NASA_GROUPS.items():
        source_name = f"nasa_{group}"

        try:
            data = fetch_nasa_group(
                location,
                start_date,
                end_date,
                group,
                parameters,
            )

            if data is None:
                failed_sources.append(source_name)
                print(f"{source_name}: {location.capital}: UNAVAILABLE")
            else:
                merge_source(data)
                print(f"{source_name}: {location.capital}: OK")

        except Exception as exc:
            failed_sources.append(source_name)
            print(f"{source_name}: {location.capital}: {exc}")

    if METEOSTAT_API_KEY:
        try:
            data = fetch_meteostat(location, start_date, end_date)

            if data is None:
                failed_sources.append("meteostat")
                print(f"meteostat: {location.capital}: UNAVAILABLE")
            else:
                merge_source(data)
                print(f"meteostat: {location.capital}: OK")

        except Exception as exc:
            failed_sources.append("meteostat")
            print(f"meteostat: {location.capital}: {exc}")
    else:
        print(f"meteostat: {location.capital}: DISABLED: NO API KEY")

    metadata = pd.DataFrame({
        "country_code": [str(location.country_code).upper()] * len(frame),
        "country": [location.country] * len(frame),
        "city": [location.capital] * len(frame),
        "latitude": [location.latitude] * len(frame),
        "longitude": [location.longitude] * len(frame),
        "elevation": [getattr(location, "elevation", None)] * len(frame),
    })

    result = pd.concat(
        [metadata.reset_index(drop=True), frame.reset_index(drop=True)],
        axis=1,
    )

    return result, failed_sources


def is_location_complete(dataset, code, start_date, end_date):
    if dataset is None or dataset.empty:
        return False

    if "country_code" not in dataset.columns or "date" not in dataset.columns:
        return False

    subset = dataset[
        dataset["country_code"].astype(str).str.upper() == code
    ].copy()

    if subset.empty:
        return False

    subset["date"] = pd.to_datetime(subset["date"], errors="coerce")
    subset = subset.dropna(subset=["date"])
    subset = subset.drop_duplicates(subset=["date"], keep="last")

    expected_dates = pd.date_range(start_date, end_date, freq="D")

    if not set(expected_dates).issubset(set(subset["date"])):
        return False

    required_prefixes = [
        "om_daily_",
        "om_hourly_",
        "nasa_sb_",
        "nasa_re_",
    ]

    for prefix in required_prefixes:
        columns = [
            column for column in subset.columns
            if column.startswith(prefix)
        ]

        if not columns and prefix in {"om_daily_", "om_hourly_"}:
            other_prefix = (
                "om_hourly_" if prefix == "om_daily_" else "om_daily_"
            )
            columns = [
                column for column in subset.columns
                if column.startswith(other_prefix)
            ]

        if not columns or not subset[columns].notna().any(axis=1).any():
            return False

    if METEOSTAT_API_KEY:
        columns = [
            column for column in subset.columns
            if column.startswith("meteostat_")
        ]

        if not columns or not subset[columns].notna().any(axis=1).any():
            return False

    return True


def consolidate_dataset():
    if not DATASET_FILE.exists():
        return

    df = pd.read_csv(DATASET_FILE)

    if df.empty:
        return

    df = (
        df.groupby(["country_code", "date"], sort=False, dropna=False)
        .last()
        .reset_index()
    )

    if CAPITALS_FILE.exists():
        capitals = pd.read_csv(CAPITALS_FILE)

        metadata_columns = [
            "country_code",
            "country",
            "city",
            "latitude",
            "longitude",
            "elevation",
        ]

        for column in metadata_columns:
            if column not in capitals.columns:
                capitals[column] = pd.NA

        lookup = capitals[metadata_columns].copy()
        lookup["country_code"] = lookup["country_code"].astype(str).str.upper()
        lookup = lookup.drop_duplicates(
            subset=["country_code"],
            keep="first",
        )

        df["country_code"] = df["country_code"].astype(str).str.upper()

        df = df.drop(
            columns=[
                "country",
                "city",
                "latitude",
                "longitude",
                "elevation",
            ],
            errors="ignore",
        )

        df = df.merge(
            lookup,
            on="country_code",
            how="left",
            validate="many_to_one",
        )

    df = df.sort_values(
        ["country_code", "date"]
    ).reset_index(drop=True)

    df.to_csv(DATASET_FILE, index=False)


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    start_date = TEST_DATE if TEST_MODE else START_DATE
    end_date = TEST_DATE if TEST_MODE else END_DATE

    if TEST_MODE:
        DATASET_FILE.unlink(missing_ok=True)

    capitals = load_capitals()

    capitals = fill_coordinates(capitals)

    if TEST_MODE:
        capitals = capitals.head(1).copy()

    capitals = capitals.dropna(
        subset=["latitude", "longitude"]
    ).reset_index(drop=True)

    if capitals.empty:
        raise RuntimeError("No capitals have valid coordinates")

    dataset = None

    if not TEST_MODE and DATASET_FILE.exists():
        try:
            dataset = pd.read_csv(DATASET_FILE)
        except (OSError, ValueError, pd.errors.ParserError):
            dataset = None

    pending = []

    for location in capitals.itertuples(index=False):
        code = str(location.country_code).upper()

        if is_location_complete(dataset, code, start_date, end_date):
            print(f"Skipping completed location: {location.capital}")
            continue

        pending.append(location)

    print(f"Capitals to process: {len(pending)}")
    print(f"Date range: {start_date} to {end_date}")

    if not METEOSTAT_API_KEY:
        print("Meteostat disabled: METEOSTAT_API_KEY is missing from .env")

    for number, location in enumerate(pending, start=1):
        print(
            f"\n[{number}/{len(pending)}] "
            f"{location.capital}, {location.country}"
        )

        try:
            frame, failed_sources = fetch_location(
                location,
                start_date,
                end_date,
            )

            frame.to_csv(
                DATASET_FILE,
                mode="a",
                header=not DATASET_FILE.exists(),
                index=False,
            )

            if failed_sources:
                print(
                    f"INCOMPLETE: {location.capital}; "
                    f"failed sources: {', '.join(failed_sources)}"
                )
                print(
                    "Stopping to avoid processing more capitals "
                    "with missing source data."
                )
                break

            print(f"Saved complete data for {location.capital}")

        except KeyboardInterrupt:
            print("\nInterrupted. Saved locations remain in the dataset.")
            raise

        except Exception as exc:
            print(f"Location failed: {location.capital}: {exc}")
            print("Stopping to avoid silently skipping a location.")
            break

    consolidate_dataset()

    if DATASET_FILE.exists():
        df = pd.read_csv(DATASET_FILE)
        print(f"\nDataset saved to {DATASET_FILE}")
        print(f"Rows: {len(df):,}")
        print(f"Columns: {len(df.columns)}")
        print(f"Capitals: {df['country_code'].nunique()}")
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")

    print("\nRate-limit summary:")

    for api_name, state in RATE_LIMIT_STATE.items():
        print(
            f"{api_name}: {state['total_429']} rate limits; "
            f"next backoff {state['next_wait']}s"
        )


if __name__ == "__main__":
    main()
