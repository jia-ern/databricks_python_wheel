import importlib.metadata
import sys


def run_entry_point(package_name: str, entry_point_name: str):
    eps = importlib.metadata.entry_points()
    console_scripts = eps.select(group="console_scripts")

    entry_point = next(
        (
            ep
            for ep in console_scripts
            if ep.name == entry_point_name and ep.dist.name == package_name
        ),
        None,
    )

    if not entry_point:
        raise Exception(
            f"Entry point '{entry_point_name}' not found in package '{package_name}'."
        )

    return entry_point.load()


if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise Exception(
            "Usage: python package_runner.py <package_name> <entry_point_name> [args...]"
        )

    package = sys.argv[1]
    entry_point = sys.argv[2]

    sys.argv = sys.argv[2:]

    main_func = run_entry_point(package, entry_point)
    main_func()
