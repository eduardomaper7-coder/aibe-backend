from math import cos, radians

def meters_to_lat(meters: float) -> float:
    return meters / 111320.0

def meters_to_lng(meters: float, lat: float) -> float:
    return meters / (111320.0 * cos(radians(lat)))

def generate_grid(center_lat: float, center_lng: float, grid_size: int = 5, spacing_meters: int = 500):
    if grid_size % 2 == 0:
        raise ValueError("grid_size must be odd, e.g. 3, 5, 7")

    half = grid_size // 2
    points = []
    counter = 1

    lat_step = meters_to_lat(spacing_meters)
    lng_step = meters_to_lng(spacing_meters, center_lat)

    for row in range(-half, half + 1):
        for col in range(-half, half + 1):
            lat = center_lat + (row * lat_step)
            lng = center_lng + (col * lng_step)
            points.append({
                "lat": round(lat, 6),
                "lng": round(lng, 6),
                "label": f"P{counter}"
            })
            counter += 1

    return points