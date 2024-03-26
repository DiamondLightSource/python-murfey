from pydantic import BaseModel


class SPAParameters(BaseModel):
    nr_iter_2d: int = 25
    nr_iter_3d: int = 25
    nr_iter_ini_model: int = 100
    batch_size_2d: int = 50000
    nr_classes_2d: int = 50
    nr_classes_3d: int = 4
    downscale: bool = True
    do_icebreaker_jobs: bool = True
    fraction_of_classes_to_remove_2d: float = 0.7
    nr_picks_before_diameter: int = 10000
    bfactor_min_particles: int = 2000


default_spa_parameters = SPAParameters()
