from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List


@dataclass
class SkysparkEquipment:
    dis: str
    siteRef: str
    refName: str
    id: str

    @classmethod
    def from_zinc_json(cls, json_data: dict) -> "SkysparkEquipment":
        return cls(
            dis=json_data["dis"],
            siteRef=json_data["siteRef"],
            refName=json_data["refName"],
            id=json_data["id"]["val"],
        )

    def __hash__(self):
        return hash(f"{self.dis}-{self.siteRef}-{self.refName}")


@dataclass
class SkysparkSite:
    dis: str
    id: str
    # refName: str

    @classmethod
    def from_zinc_json(cls, json_dict) -> "SkysparkSite":
        if isinstance(json_dict.get("id"), dict):
            id = json_dict["id"]["val"]
        elif isinstance(json_dict.get("id"), str):
            id = json_dict["id"]
        return cls(
            dis=json_dict["dis"],
            id=id
            # refName = json_dict['refName']
        )


@dataclass
class SkysparkPoint:
    dis: str
    siteRef: str
    equipRef: str
    marker_tags: list
    kv_tags: dict
    # refName: str
    id: str

    @classmethod
    def from_zinc_json(cls, json_dict: dict) -> "SkysparkPoint":
        markers = []
        kv_tags = {}
        id = json_dict.pop("id")
        dis = json_dict.pop("dis", "")
        if isinstance(id, dict):
            new_id = id.copy()
            id = new_id["val"]
            dis = new_id["dis"]
        siteRef = json_dict.pop("siteRef", None)
        equipRef = json_dict.pop("equipRef", None)
        for key, value in json_dict.items():
            if not isinstance(value, dict):
                if value != "m:":
                    kv_tags[key] = value
                else:
                    markers.append(key)
            elif value.get("_kind") == "marker":
                markers.append(key)
            elif value.get("_kind") == "ref":
                kv_tags["key"] = value
        return cls(
            id=id,
            dis=dis,
            siteRef=siteRef,
            equipRef=equipRef,
            marker_tags=markers,
            kv_tags=kv_tags,
        )


@dataclass
class SkysparkSample:
    id: str
    time: datetime
    value: float
    refName: str


@dataclass
class SimpleSample:
    name: str
    time: datetime
    value: float


@dataclass
class SimplePoint:
    name: str
    point_type: str
    marker_tags: List[str]
    kv_tags: Dict

    def from_api_model(api_point: dict):
        return SimplePoint(
            name=api_point["name"],
            point_type=api_point["point_type"],
            marker_tags=api_point["marker_tags"],
            kv_tags=api_point["kv_tags"],
        )
