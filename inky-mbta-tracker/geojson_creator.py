import logging
import os
from asyncio import Runner, sleep
from datetime import datetime
from pathlib import Path
from shutil import copyfile
from tempfile import TemporaryDirectory
from typing import Optional

from config import Config
from geojson import Feature, FeatureCollection, Point, dumps
from pydantic import ValidationError
from pygit2 import Repository, Signature, clone_repository
from pygit2.enums import FileStatus
from redis import ResponseError
from redis.asyncio import Redis
from schedule_tracker import VehicleRedisSchema
from tenacity import before_sleep_log, retry, wait_exponential
from zoneinfo import ZoneInfo

logger = logging.getLogger("geojson")


def ret_color(vehicle: VehicleRedisSchema):
    if vehicle.route.startswith("Green"):
        return "#008150"
    if vehicle.route.startswith("Blue"):
        return "#2F5DA6"
    if vehicle.route.startswith("CR"):
        return "#7B388C"
    if vehicle.route.startswith("Red") or vehicle.route.startswith("Mattapan"):
        return "#FA2D27"
    if vehicle.route.startswith("Orange"):
        return "#FD8A03"


class GitClient:
    git_repo_local: Path
    git_repo_url: str
    git_email: str
    auth_user: str
    auth_token: str
    repo: Optional[Repository]

    def __init__(
        self,
        git_repo_local: Path,
        git_repo_url: str,
        auth_user: str,
        auth_token: str,
        git_email: str,
    ):
        self.git_repo_local = git_repo_local
        self.git_repo_url = git_repo_url
        self.auth_user = auth_user
        self.auth_token = auth_token
        self.git_email = git_email

    def clone(self):
        clone_repository(
            url=f"https://{self.auth_user}:{self.auth_token}@{self.git_repo_url}",
            path=self.git_repo_local,
        )
        self.repo = Repository(self.git_repo_local)

    def commit_and_push(self, txt_file_name: Path):
        remote_name = "origin"

        dest = (self.git_repo_local / "vehicles.json").absolute()
        copyfile(txt_file_name, dest)
        index = self.repo.index

        status = self.repo.status()
        for filepath, flags in status.items():
            if flags != FileStatus.CURRENT:
                index.add_all()
                index.write()

                ref = "HEAD"
                author = Signature(self.auth_user, self.git_email)
                message = f"update for {datetime.now().astimezone(ZoneInfo('US/Eastern')).strftime('%c')}"
                tree = index.write_tree()
                self.repo.create_commit(
                    ref, author, author, message, tree, [self.repo.head.target]
                )

                branch = "main"
                self.repo.remotes[remote_name].push((["+refs/heads/{}".format(branch)]))


@retry(
    wait=wait_exponential(multiplier=1, min=1, max=10),
    before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
)
async def create_json(config: Config):
    r = Redis(
        host=os.environ.get("IMT_REDIS_ENDPOINT"),
        port=os.environ.get("IMT_REDIS_PORT", "6379"),
        password=os.environ.get("IMT_REDIS_PASSWORD"),
    )

    with TemporaryDirectory() as tmpdir:
        git_client = None
        if (
            config.vehicle_git_repo
            and config.vehicle_git_user
            and config.vehicle_git_token
            and config.vehicle_git_email
        ):
            git_client = GitClient(
                git_repo_url=config.vehicle_git_repo,
                git_repo_local=Path(tmpdir),
                auth_user=config.vehicle_git_user,
                auth_token=config.vehicle_git_token,
                git_email=config.vehicle_git_email,
            )
            git_client.clone()
        while True:
            try:
                vehicle_ids = await r.scan(0, match="vehicle*", count=300)
                features = list[Feature]()
                pl = r.pipeline()

                if vehicle_ids:
                    for vehicle in vehicle_ids[1]:
                        dec_v = vehicle.decode("utf-8")
                        if dec_v:
                            await pl.get(vehicle)

                    results = await pl.execute()
                    for result in results:
                        vehicle_info = VehicleRedisSchema.model_validate_json(
                            strict=False, json_data=result
                        )
                        point = Point((vehicle_info.longitude, vehicle_info.latitude))
                        feature = Feature(
                            geometry=point,
                            id=vehicle_info.id,
                            properties={
                                "route": vehicle_info.route,
                                "status": vehicle_info.current_status,
                                "marker-size": "medium",
                                "marker-symbol": "rail",
                                "marker-color": ret_color(vehicle_info),
                                "speed": vehicle_info.speed,
                                "direction": vehicle_info.direction_id,
                                "id": vehicle_info.id,
                                "stop": vehicle_info.stop,
                                "update_time": datetime.now()
                                .astimezone(ZoneInfo("US/Eastern"))
                                .strftime("%c"),
                            },
                        )
                        features.append(feature)
                    write_file = Path(
                        os.environ.get("IMT_JSON_WRITE_FILE", "./imt-out.json")
                    )
                    write_file.unlink(missing_ok=True)
                    with open(
                        write_file,
                        "w",
                    ) as file:
                        file.write(
                            dumps(
                                FeatureCollection(
                                    features=sorted(
                                        features, key=lambda d: d["properties"]["id"]
                                    )
                                ),
                                sort_keys=True,
                                indent=2,
                            )
                        )

                if git_client:
                    git_client.commit_and_push(write_file)
                await sleep(45)
            except ResponseError as err:
                logger.error("unable to run redis command", exc_info=err)
            except ValidationError as err:
                logger.error("unable to validate model", exc_info=err)


def run(config: Config):
    with Runner() as runner:
        runner.run(create_json(config))