import os
from base64 import urlsafe_b64decode, urlsafe_b64encode
from datetime import datetime, timedelta
from typing import List, Union

import requests

# from ace_api.endpoints import get_configured_points, get_site_timeseries
# from ace_api.models import BACnetPoint, Point, Site
from scramp import ScramClient

from ace_skyspark.skyspark.models import (
    SkysparkEquipment,
    SkysparkPoint,
    SkysparkSample,
    SkysparkSite,
)
from ace_skyspark.skyspark.ops import (
    render_commit_add_equips,
    render_commit_add_points,
    render_his_write_grid,
    render_sites_add,
)

DEFAULT_HEADERS = {}


def process_response(res: requests.Response):
    if res.status_code == 200:
        try:
            return res.json().get("rows", [])
        except requests.JSONDecodeError:
            print(f"error decoding response")
    else:
        print(f"error decoding response, status: {res.status_code}")
        return []


def get_scram_token(url: str, username: str, password: str):
    if username == "" and password == "":
        raise ValueError("Username and password must be provided")
    b64user = urlsafe_b64encode(username.encode("utf-8")).decode("utf-8").split("=")[0]
    hello_res = requests.get(
        url, headers={"Authorization": "HELLO username=" + b64user}
    )
    handshakeToken = (
        hello_res.headers.get("www-authenticate").split("=")[1].split(",")[0]
    )

    c = ScramClient(["SCRAM-SHA-256"], username, password)
    client_first = c.get_client_first()
    res = requests.get(
        url,
        headers={
            "Authorization": f"SCRAM handshakeToken={handshakeToken}, hash=SHA-256, data={urlsafe_b64encode(client_first.encode('utf-8')).decode('utf-8').rstrip('=')}"
        },
    )
    assert (
        res.status_code == 401
    ), f"Expected 401 response, got: {res.status_code} - {res.text}"
    c.set_server_first(
        urlsafe_b64decode(
            res.headers.get("www-authenticate").split("data=")[1].split(",")[0] + "==="
        ).decode("utf-8")
    )
    client_final = c.get_client_final()
    res = requests.get(
        url,
        headers={
            "Authorization": f"SCRAM handshakeToken={handshakeToken}, hash=SHA-256, data={urlsafe_b64encode(client_final.encode('utf-8')).decode('utf-8')}"
        },
    )
    c.set_server_final(
        urlsafe_b64decode(
            res.headers.get("authentication-info").split("data=")[1].split(",")[0]
            + "=="
        ).decode("utf-8")
    )
    return res.headers.get("authentication-info").split("authToken=")[1].split(",")[0]


def post_commit_grid(url: str, auth_token: str, grid: str):
    res = requests.post(
        url,
        data=grid,
        headers={
            "Authorization": f"Bearer authToken={auth_token}",
            "accept": "application/json",
            "content-type": "text/zinc",
        },
    )
    return res


def post_grid(url: str, auth_token: str, grid: str):
    res = requests.post(
        url,
        data=grid,
        headers={
            "Authorization": f"Bearer authToken={auth_token}",
            "accept": "application/json",
            "content-type": "text/zinc",
        },
    )
    return res


def get_filter(url: str, auth_token: str, project: str, filter: str) -> dict:
    res = requests.get(
        f"{url}/{project}/read",
        params={"filter": filter},
        headers={
            "Authorization": f"Bearer authToken={auth_token}",
            "accept": "application/json",
            "content-type": "text/zinc",
        },
    )
    return res.json().get("rows", [])


def get_filtered_points(
    url: str, auth_token: str, project: str, filter: str
) -> List[SkysparkPoint]:
    results = get_filter(url, auth_token, project, f"{filter} and point")
    if len(results) > 0:
        return [SkysparkPoint.from_zinc_json(point) for point in results]
    else:
        return []


def get_sites(url: str, auth_token: str, project: str) -> List[SkysparkSite]:
    return [
        SkysparkSite.from_zinc_json(site)
        for site in get_filter(url, auth_token, project, "site")
    ]


def get_points(url: str, auth_token: str, project: str) -> List[SkysparkPoint]:
    return [
        SkysparkPoint.from_zinc_json(point)
        for point in get_filter(url, auth_token, project, "point")
    ]


def create_sites(
    url: str, auth_token: str, points: List[SkysparkPoint]
) -> List[SkysparkSite]:
    points_to_create_sites = [
        point for point in points if "haystack_siteRef" not in point.kv_tags
    ]
    if len(points_to_create_sites) > 0:
        site_refs = set()
        for point in points_to_create_sites:
            site_ref = point.name.split("/")[1]
            site_refs.add(site_ref)
        sites = [
            SkysparkSite(name=site_ref, nice_name="", client="")
            for site_ref in site_refs
        ]
        grid = render_sites_add(sites)
        res = post_commit_grid(url, auth_token, grid)
        return [SkysparkSite.from_zinc_json(row) for row in res.json()["rows"]]


def create_equips(
    url: str, auth_token: str, points: List[SkysparkPoint]
) -> List[SkysparkEquipment]:
    points_to_create_equips = [
        point for point in points if "haystack_equipRef" not in point.kv_tags
    ]
    if len(points_to_create_equips) > 0:
        grid = render_commit_add_equips(points_to_create_equips)
        res = post_commit_grid(url, auth_token, grid)
        return [SkysparkEquipment.from_zinc_json(row) for row in res.json()["rows"]]


def update_points_with_existing_sites(
    points: List[SkysparkPoint],
) -> List[SkysparkPoint]:
    site_ref_map = {}
    for point in points:
        site_ref = point.name.split("/")[1]
        if "haystack_siteRef" in point.kv_tags:
            site_ref_map[site_ref] = point.kv_tags["haystack_siteRef"]
    for point in points:
        site_ref = point.name.split("/")[1]
        if "haystack_siteRef" not in point.kv_tags and site_ref in site_ref_map:
            point.kv_tags.update({"haystack_siteRef": site_ref_map[site_ref]})
    return points


def update_points_with_sites(
    url: str, auth_token: str, points: List[SkysparkPoint]
) -> List[SkysparkPoint]:
    existing_site_points = update_points_with_existing_sites(points)
    sites = create_sites(url, auth_token, existing_site_points)
    site_ref_map = {site.refName: site for site in sites}
    if sites:
        for point in existing_site_points:
            site_ref = point.name.split("/")[1]
            if "haystack_siteRef" not in point.kv_tags and site_ref in site_ref_map:
                point.kv_tags.update({"haystack_siteRef": site_ref_map[site_ref].id})
    return existing_site_points


def update_points_with_existing_equips(
    points: List[SkysparkPoint],
) -> List[SkysparkPoint]:
    equip_ref_map = {}
    for point in points:
        equip_ref = point.name.split("/")[2]
        if "haystack_equipRef" in point.kv_tags:
            equip_ref_map[equip_ref] = point.kv_tags["haystack_equipRef"]
    for point in points:
        equip_ref = point.name.split("/")[2]
        if "haystack_equipRef" not in point.kv_tags and equip_ref in equip_ref_map:
            point.kv_tags.update({"haystack_equipRef": equip_ref_map[equip_ref]})
    return points


def update_points_with_equips(
    url: str, auth_token: str, points: List[SkysparkPoint]
) -> List[SkysparkPoint]:
    existing_equip_points = update_points_with_existing_equips(points)
    equips = create_equips(url, auth_token, existing_equip_points)
    if equips:
        equip_ref_map = {equip.refName: equip for equip in equips}
        for point in existing_equip_points:
            equip_ref = point.name.split("/")[2]
            if equip_ref in equip_ref_map:
                point.kv_tags.update({"haystack_equipRef": equip_ref_map[equip_ref].id})
    return existing_equip_points


def create_points(
    url: str, auth_token: str, points: List[SkysparkPoint]
) -> List[SkysparkPoint]:
    points_to_create_points = [
        point for point in points if "haystack_entityRef" not in point.kv_tags
    ]
    if len(points_to_create_points) > 0:
        grid = render_commit_add_points(points_to_create_points)
        res = post_commit_grid(url, auth_token, grid)
        return [SkysparkPoint.from_zinc_json(row) for row in res.json()["rows"]]


def update_points_with_entities(
    url: str, auth_token: str, points: List[SkysparkPoint]
) -> List[SkysparkPoint]:
    skyspark_points = create_points(url, auth_token, points)
    point_ref_map = {point.refName: point for point in skyspark_points}
    if skyspark_points:
        for point in points:
            if (
                "haystack_entityRef" not in point.kv_tags
                and point.name in point_ref_map
            ):
                point.kv_tags.update(
                    {"haystack_entityRef": point_ref_map[point.name].id}
                )
    return points


def his_write_samples(
    url: str, auth_token: str, samples: List[SkysparkSample]
) -> List[SkysparkSample]:
    if len(samples) > 0:
        grid = render_his_write_grid(samples)
        res = post_grid(url, auth_token, grid)
        return res
