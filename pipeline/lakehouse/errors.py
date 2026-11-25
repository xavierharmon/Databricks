class StageFailure(Exception):
    """A step could not do its job (missing file, missing parent table, missing transform): an operational failure, not a data-quality one."""
