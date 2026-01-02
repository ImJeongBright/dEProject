import math

def mapToGrid(lat, lon, code=0):
    """
    위경도(Lat/Lon)를 기상청 격자(NX, NY)로 변환하거나, 격자를 위경도로 변환합니다.
    (Lambert Conformal Conic Projection)

    Args:
        lat (float): 위도 (또는 변환 모드에 따라 X좌표)
        lon (float): 경도 (또는 변환 모드에 따라 Y좌표)
        code (int): 0 (Lat/Lon -> Grid), 1 (Grid -> Lat/Lon)

    Returns:
        dict: {'x': grid_x, 'y': grid_y} (code=0)
              {'lat': lat, 'lng': lon} (code=1)
    """


    RE = 6371.00877
    GRID = 5.0
    SLAT1 = 30.0
    SLAT2 = 60.0
    OLON = 126.0
    OLAT = 38.0
    XO = 43
    YO = 136


    DEGRAD = math.pi / 180.0
    RADDEG = 180.0 / math.pi

    re = RE / GRID
    slat1 = SLAT1 * DEGRAD
    slat2 = SLAT2 * DEGRAD
    olon = OLON * DEGRAD
    olat = OLAT * DEGRAD

    sn = math.tan(math.pi * 0.25 + slat2 * 0.5) / math.tan(math.pi * 0.25 + slat1 * 0.5)
    sn = math.log(math.cos(slat1) / math.cos(slat2)) / math.log(sn)
    sf = math.tan(math.pi * 0.25 + slat1 * 0.5)
    sf = math.pow(sf, sn) * math.cos(slat1) / sn
    ro = math.tan(math.pi * 0.25 + olat * 0.5)
    ro = re * sf / math.pow(ro, sn)

    rs = {}

    if code == 0:
        ra = math.tan(math.pi * 0.25 + lat * DEGRAD * 0.5)
        ra = re * sf / math.pow(ra, sn)
        theta = lon * DEGRAD - olon
        if theta > math.pi:
            theta -= 2.0 * math.pi
        if theta < -math.pi:
            theta += 2.0 * math.pi
        theta *= sn

        x = (ra * math.sin(theta)) + XO + 0.5
        y = (ro - ra * math.cos(theta)) + YO + 0.5

        rs['x'] = int(x)
        rs['y'] = int(y)

    else:
        xn = lat - XO
        yn = ro - lon + YO
        ra = math.sqrt(xn * xn + yn * yn)
        if sn < 0.0:
            ra = -ra
        alat = math.pow((re * sf / ra), (1.0 / sn))
        alat = 2.0 * math.atan(alat) - math.pi * 0.5

        if math.fabs(xn) <= 0.0:
            theta = 0.0
        else:
            if math.fabs(yn) <= 0.0:
                theta = math.pi * 0.5
                if xn < 0.0:
                    theta = -theta
            else:
                theta = math.atan2(xn, yn)

        alon = theta / sn + olon
        rs['lat'] = alat * RADDEG
        rs['lng'] = alon * RADDEG

    return rs

if __name__ == "__main__":


    seoul_lat = 37.5665
    seoul_lon = 126.9780
    result = mapToGrid(seoul_lat, seoul_lon)
    print(f"Seoul City Hall ({seoul_lat}, {seoul_lon}) -> Grid: {result}")
