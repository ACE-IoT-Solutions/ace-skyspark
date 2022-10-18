import logging
from typing import List, Union
from pytz import UTC

from ace_skyspark.skyspark.models import (
    SkysparkEquipment,
    SkysparkPoint,
    SkysparkSample,
    SkysparkSite,
)

_log = logging.getLogger(__name__)


def render_his_write(sample: SkysparkSample, point: SkysparkPoint) -> str:
    entity_id = (
        f"@{point.kv_tags['haystack_entityRef'][2:]}"
        if point.kv_tags["haystack_entityRef"].startswith("r:")
        else point.kv_tags["haystack_entityRef"]
    )
    return f'hisWrite({{ts: parseDateTime("{sample.time.isoformat()}", "YYYY-MM-DDThh:mm:ss.f"), val: {sample.value}}}, @{entity_id})'


def render_his_write_sample(sample: SkysparkSample, tz=None) -> str:
    # pytz supports all the zones project haystack uses except they're named different
    # the workaround is to split on '/' this works for all but the 'GMT+' zones
    tz = sample.time.tzinfo.zone.split("/")[-1]
    return f'"hisWrite({{ts: parseDateTime(\\"{sample.time.replace(microsecond=0).isoformat()}\\", \\"YYYY-MM-DDThh:mm:ssz\\", \\"{tz}\\"), val: {str(sample.value).lower()}}}, @{sample.id})"'


def render_his_write_grid_point_section(
    samples: List[SkysparkSample], point: SkysparkPoint
) -> str:
    section = ""
    for sample in samples:
        if sample.name == point.name:
            section += render_his_write(sample, point) + "\n"
        else:
            _log.warning(
                f"Sample name {sample.name} does not match point name {point.name}"
            )
    return section


def render_his_write_grid(samples: List[SkysparkSample]) -> str:
    grid = ""
    grid += """ver:"3.0"\n"""
    grid += "expr\n"
    for sample in samples:
        grid += render_his_write_sample(sample) + "\n"
    return grid


def render_point_add(point: SkysparkPoint) -> str:
    return f""""{point.name}", @{point.kv_tags['haystack_equipRef']}, @{point.kv_tags['haystack_siteRef']}, "UTC", "Number", "{point.name}",M,M,M"""


def render_commit_add_points(points: List[SkysparkPoint]) -> str:
    columns = [
        "dis",
        "equipRef",
        "siteRef",
        "tz",
        "kind",
        "refName",
        "cur",
        "his",
        "point",
    ]
    grid = ""
    grid += """ver:"3.0" commit:"add"\n"""
    grid += ",".join(columns) + "\n"
    for point in points:
        grid += render_point_add(point) + "\n"
    return grid


def render_equip_add(equip: str, siteRef) -> str:
    return f'"{equip}"'


def parse_equips_from_points(points: List[SkysparkPoint]) -> List[SkysparkEquipment]:
    equips = set()
    for point in points:
        if not point.kv_tags.get("haystack_equipRef"):
            ace_equip_name = point.name.split("/")[2]
            equips.add(
                SkysparkEquipment(
                    dis=ace_equip_name,
                    siteRef=point.kv_tags.get("haystack_siteRef"),
                    refName=ace_equip_name,
                    id=None,
                )
            )
    return equips


def render_commit_add_equips(points: List[SkysparkPoint]) -> str:
    columns = [
        "dis",
        "siteRef",
        "tz",
        "refName",
        "equip",
    ]
    grid = ""
    grid += """ver:"3.0" commit:"add"\n"""
    grid += ", ".join(columns) + "\n"
    equips = parse_equips_from_points(points)
    for equip in equips:
        grid += f""""{equip.dis}",@{equip.siteRef},"UTC","{equip.refName}",M""" + "\n"
    return grid


def render_sites_add(sites: List[SkysparkSite]) -> str:
    grid = """ver: "3.0" commit: "add"\n"""
    grid += """dis, tz, refName, site\n"""
    for site in sites:
        grid += f""""{site.name}", "UTC", "{site.name}", M\n"""
    return grid
