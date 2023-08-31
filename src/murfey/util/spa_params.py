from pydantic import BaseModel


class SPAParameters(BaseModel):
    nr_iter_2d: int = 25
    batch_size_2d: int = 50000
    nr_classes_2d: int = 50
    downscale: bool = True
    do_icebreaker_jobs: bool = True
    fraction_of_classes_to_remove_2d: float = 0.5
    nr_picks_before_diameter: int = 10000


default_spa_parameters = SPAParameters()
