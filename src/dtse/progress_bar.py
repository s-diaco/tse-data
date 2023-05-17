"""
progress bar for dtse
"""


class ProgressBar:
    """
    Manage progressbar data
    """

    prog_n: int
    prog_tot: int
    prog_succ_req: int
    prog_req: int

    def prog_func(self):
        # TODO: delete
        pass

    def __init__(self, **kwargs) -> None:
        """
        initialize progressbar data
        """
        default_settings = {
            "prog_func": None,
            "prog_n": 0,
            "prog_tot": 100,
            "prog_succ_req": None,
            "prog_req": None,
        }

        bad_keys = [k for k in kwargs if k not in default_settings]
        if bad_keys:
            raise TypeError(f"Invalid arguments for ProgressBar.__init__: {bad_keys}")
        self.update(**kwargs)

    def update(self, **kwargs) -> None:
        """
        update progressbar data
        """
        self.__dict__.update(kwargs)
