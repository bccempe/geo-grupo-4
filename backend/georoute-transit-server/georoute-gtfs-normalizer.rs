use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

use csv::{ReaderBuilder, StringRecord, WriterBuilder};

const STOP_SHAPE_THRESHOLD_M: f64 = 50.0;
const BUS_SPEED_M_S: f64 = 15.0 / 3.6;

#[derive(Clone)]
struct ShapePoint {
    shape: u32,
    lon: f64,
    lat: f64,
    cumulative_m: f64,
}

struct Stop {
    id: String,
    lon: f64,
    lat: f64,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() == 2 && matches!(args[1].as_str(), "-h" | "--help") {
        println!("Usage: georoute-gtfs-normalizer <source-gtfs> <output-gtfs>");
        return Ok(());
    }
    if args.len() != 3 {
        return Err("expected source and output GTFS directories".into());
    }

    let source = PathBuf::from(&args[1]);
    let output = PathBuf::from(&args[2]);
    normalize(&source, &output)
}

fn normalize(source: &Path, output: &Path) -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(output)?;
    for file in [
        "stops.txt",
        "trips.txt",
        "frequencies.txt",
        "calendar.txt",
        "calendar_dates.txt",
    ] {
        let input = source.join(file);
        if input.exists() {
            fs::copy(input, output.join(file))?;
        }
    }

    let bus_routes = read_bus_routes(source)?;
    let (trip_shapes, bus_shapes) = read_bus_trip_shapes(source, &bus_routes)?;
    let frequency_trips = read_frequency_trips(source)?;
    let (shape_names, points, grid) = read_shape_points(source, &bus_shapes)?;
    let stops = read_stops(source)?;
    let shape_stops = assign_stops_to_shapes(&stops, &points, &grid);

    let stop_times_path = source.join("stop_times.txt");
    let mut source_stop_times = ReaderBuilder::new()
        .flexible(true)
        .from_path(&stop_times_path)?;
    let headers = source_stop_times.headers()?.clone();
    let trip_ix = column(&headers, "trip_id", "stop_times.txt")?;
    let arrival_ix = column(&headers, "arrival_time", "stop_times.txt")?;
    let departure_ix = column(&headers, "departure_time", "stop_times.txt")?;
    let stop_ix = column(&headers, "stop_id", "stop_times.txt")?;
    let sequence_ix = column(&headers, "stop_sequence", "stop_times.txt")?;

    let mut writer = WriterBuilder::new().from_path(output.join("stop_times.txt"))?;
    writer.write_record([
        "trip_id",
        "arrival_time",
        "departure_time",
        "stop_id",
        "stop_sequence",
    ])?;

    let mut existing_trips = HashSet::new();
    for record in source_stop_times.records() {
        let record = record?;
        let trip_id = value(&record, trip_ix);
        if trip_id.is_empty() {
            continue;
        }
        existing_trips.insert(trip_id.to_string());
        writer.write_record([
            trip_id,
            value(&record, arrival_ix),
            value(&record, departure_ix),
            value(&record, stop_ix),
            value(&record, sequence_ix),
        ])?;
    }

    let mut synthetic_trips = 0usize;
    let mut synthetic_rows = 0usize;
    for (trip_id, shape_id) in trip_shapes {
        if existing_trips.contains(&trip_id) || !frequency_trips.contains(&trip_id) {
            continue;
        }
        let Some(&shape_index) = shape_names.get(&shape_id) else {
            continue;
        };
        let Some(ordered_stops) = shape_stops.get(&shape_index) else {
            continue;
        };
        if ordered_stops.len() < 2 {
            continue;
        }

        let first_distance = ordered_stops[0].1;
        for (index, (stop_id, cumulative_m)) in ordered_stops.iter().enumerate() {
            let seconds = ((cumulative_m - first_distance) / BUS_SPEED_M_S)
                .max(0.0)
                .round() as i32;
            let time = format_gtfs_time(seconds);
            writer.write_record([
                trip_id.as_str(),
                time.as_str(),
                time.as_str(),
                stop_id.as_str(),
                &(index + 1).to_string(),
            ])?;
            synthetic_rows += 1;
        }
        synthetic_trips += 1;
    }
    writer.flush()?;

    eprintln!(
        "Normalized GTFS: {} bus shapes, {} assigned stops, {} synthetic trips, {} stop-time rows",
        shape_stops.len(),
        shape_stops.values().map(Vec::len).sum::<usize>(),
        synthetic_trips,
        synthetic_rows
    );
    Ok(())
}

fn read_bus_routes(source: &Path) -> Result<HashSet<String>, Box<dyn Error>> {
    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_path(source.join("routes.txt"))?;
    let headers = reader.headers()?.clone();
    let route_ix = column(&headers, "route_id", "routes.txt")?;
    let type_ix = column(&headers, "route_type", "routes.txt")?;
    let mut routes = HashSet::new();
    for record in reader.records() {
        let record = record?;
        let route_type = value(&record, type_ix).parse::<u16>().unwrap_or(3);
        if !matches!(route_type, 0 | 1) {
            routes.insert(value(&record, route_ix).to_string());
        }
    }
    Ok(routes)
}

fn read_bus_trip_shapes(
    source: &Path,
    bus_routes: &HashSet<String>,
) -> Result<(HashMap<String, String>, HashSet<String>), Box<dyn Error>> {
    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_path(source.join("trips.txt"))?;
    let headers = reader.headers()?.clone();
    let route_ix = column(&headers, "route_id", "trips.txt")?;
    let trip_ix = column(&headers, "trip_id", "trips.txt")?;
    let shape_ix = column(&headers, "shape_id", "trips.txt")?;
    let mut trips = HashMap::new();
    let mut shapes = HashSet::new();
    for record in reader.records() {
        let record = record?;
        if !bus_routes.contains(value(&record, route_ix)) {
            continue;
        }
        let trip_id = value(&record, trip_ix);
        let shape_id = value(&record, shape_ix);
        if !trip_id.is_empty() && !shape_id.is_empty() {
            trips.insert(trip_id.to_string(), shape_id.to_string());
            shapes.insert(shape_id.to_string());
        }
    }
    Ok((trips, shapes))
}

fn read_frequency_trips(source: &Path) -> Result<HashSet<String>, Box<dyn Error>> {
    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_path(source.join("frequencies.txt"))?;
    let headers = reader.headers()?.clone();
    let trip_ix = column(&headers, "trip_id", "frequencies.txt")?;
    let mut trips = HashSet::new();
    for record in reader.records() {
        let record = record?;
        let trip_id = value(&record, trip_ix);
        if !trip_id.is_empty() {
            trips.insert(trip_id.to_string());
        }
    }
    Ok(trips)
}

type ShapeGrid = HashMap<(i32, i32), Vec<usize>>;

fn read_shape_points(
    source: &Path,
    bus_shapes: &HashSet<String>,
) -> Result<(HashMap<String, u32>, Vec<ShapePoint>, ShapeGrid), Box<dyn Error>> {
    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_path(source.join("shapes.txt"))?;
    let headers = reader.headers()?.clone();
    let shape_ix = column(&headers, "shape_id", "shapes.txt")?;
    let lat_ix = column(&headers, "shape_pt_lat", "shapes.txt")?;
    let lon_ix = column(&headers, "shape_pt_lon", "shapes.txt")?;
    let sequence_ix = column(&headers, "shape_pt_sequence", "shapes.txt")?;

    // El orden estable resuelve igual los empates entre trazados superpuestos.
    let mut raw: BTreeMap<String, Vec<(i64, f64, f64)>> = BTreeMap::new();
    for record in reader.records() {
        let record = record?;
        let shape_id = value(&record, shape_ix);
        if !bus_shapes.contains(shape_id) {
            continue;
        }
        let (Ok(lat), Ok(lon), Ok(sequence)) = (
            value(&record, lat_ix).parse::<f64>(),
            value(&record, lon_ix).parse::<f64>(),
            value(&record, sequence_ix).parse::<i64>(),
        ) else {
            continue;
        };
        raw.entry(shape_id.to_string())
            .or_default()
            .push((sequence, lon, lat));
    }

    let mut shape_names = HashMap::new();
    let mut points = Vec::new();
    for (shape_id, mut shape_points) in raw {
        shape_points.sort_by_key(|point| point.0);
        let shape = shape_names.len() as u32;
        shape_names.insert(shape_id, shape);
        let mut cumulative_m = 0.0;
        let mut previous = None;
        for (_, lon, lat) in shape_points {
            if let Some((previous_lon, previous_lat)) = previous {
                cumulative_m += haversine_m(previous_lon, previous_lat, lon, lat);
            }
            points.push(ShapePoint {
                shape,
                lon,
                lat,
                cumulative_m,
            });
            previous = Some((lon, lat));
        }
    }

    let cell_size = STOP_SHAPE_THRESHOLD_M / 111_000.0;
    let mut grid: ShapeGrid = HashMap::new();
    for (index, point) in points.iter().enumerate() {
        grid.entry(cell(point.lon, point.lat, cell_size))
            .or_default()
            .push(index);
    }
    Ok((shape_names, points, grid))
}

fn read_stops(source: &Path) -> Result<Vec<Stop>, Box<dyn Error>> {
    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_path(source.join("stops.txt"))?;
    let headers = reader.headers()?.clone();
    let stop_ix = column(&headers, "stop_id", "stops.txt")?;
    let lat_ix = column(&headers, "stop_lat", "stops.txt")?;
    let lon_ix = column(&headers, "stop_lon", "stops.txt")?;
    let mut stops = Vec::new();
    for record in reader.records() {
        let record = record?;
        let (Ok(lat), Ok(lon)) = (
            value(&record, lat_ix).parse::<f64>(),
            value(&record, lon_ix).parse::<f64>(),
        ) else {
            continue;
        };
        stops.push(Stop {
            id: value(&record, stop_ix).to_string(),
            lon,
            lat,
        });
    }
    Ok(stops)
}

fn assign_stops_to_shapes(
    stops: &[Stop],
    points: &[ShapePoint],
    grid: &ShapeGrid,
) -> HashMap<u32, Vec<(String, f64)>> {
    let cell_size = STOP_SHAPE_THRESHOLD_M / 111_000.0;
    let mut assigned: HashMap<u32, Vec<(String, f64)>> = HashMap::new();
    for stop in stops {
        let (x, y) = cell(stop.lon, stop.lat, cell_size);
        let mut nearest: Option<(&ShapePoint, f64)> = None;
        for dx in -2..=2 {
            for dy in -2..=2 {
                let Some(candidates) = grid.get(&(x + dx, y + dy)) else {
                    continue;
                };
                for &index in candidates {
                    let point = &points[index];
                    let distance = haversine_m(stop.lon, stop.lat, point.lon, point.lat);
                    if distance <= STOP_SHAPE_THRESHOLD_M
                        && nearest.is_none_or(|(_, best)| distance < best)
                    {
                        nearest = Some((point, distance));
                    }
                }
            }
        }
        if let Some((point, _)) = nearest {
            assigned
                .entry(point.shape)
                .or_default()
                .push((stop.id.clone(), point.cumulative_m));
        }
    }

    for shape_stops in assigned.values_mut() {
        shape_stops.sort_by(|a, b| a.1.total_cmp(&b.1));
        let mut seen = HashSet::new();
        shape_stops.retain(|(stop_id, _)| seen.insert(stop_id.clone()));
    }
    assigned
}

fn column(headers: &StringRecord, name: &str, file: &str) -> Result<usize, Box<dyn Error>> {
    headers
        .iter()
        .position(|header| header.trim_start_matches('\u{feff}').trim() == name)
        .ok_or_else(|| format!("{file} missing required column '{name}'").into())
}

fn value(record: &StringRecord, index: usize) -> &str {
    record.get(index).unwrap_or("").trim()
}

fn cell(lon: f64, lat: f64, size: f64) -> (i32, i32) {
    ((lon / size).floor() as i32, (lat / size).floor() as i32)
}

fn haversine_m(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let radius_m = 6_371_000.0;
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    2.0 * radius_m * a.sqrt().asin()
}

fn format_gtfs_time(seconds: i32) -> String {
    let hours = seconds / 3_600;
    let minutes = (seconds % 3_600) / 60;
    let seconds = seconds % 60;
    format!("{hours:02}:{minutes:02}:{seconds:02}")
}
