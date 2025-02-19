"""
Helper function to automate the process of building and publishing Docker images for
Murfey using Python subprocesses.

This CLI is designed to run with Podman commands and in a bash shell that has been
configured to push to a valid Docker repo, which has to be specified using a flag.
"""

import grp
import os
import subprocess
from argparse import ArgumentParser
from pathlib import Path


def run_subprocess(cmd: list[str], src: str = "."):
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        universal_newlines=True,
        env=os.environ,
        cwd=Path(src),
    )

    # Parse stdout and stderr
    if process.stdout:
        for line in process.stdout:
            print(line, end="")
    if process.stderr:
        for line in process.stderr:
            print(line, end="")

    # Wait for process to complete
    process.wait()

    return process.returncode


# Function to build Docker image
def build_image(
    image: str,
    tag: str,
    source: str,
    destination: str,
    user_id: int,
    group_id: int,
    group_name: str,
    dry_run: bool = False,
):
    # Construct path to Dockerfile
    dockerfile = Path(source) / "Dockerfiles" / image
    if not dockerfile.exists():
        raise FileNotFoundError(
            f"Unable to find Dockerfile for {image} at {str(dockerfile)!r}"
        )

    # Construct tag
    image_path = f"{destination}/{image}"
    if tag:
        image_path = f"{image_path}:{tag}"

    # Construct bash command to build image
    build_cmd = [
        "podman build",
        f"--build-arg=userid={user_id}",
        f"--build-arg=groupid={group_id}",
        f"--build-arg=groupname={group_name}",
        "--no-cache",
        f"-f {str(dockerfile)}",
        f"-t {image_path}",
        f"{source}",
    ]
    bash_cmd = ["bash", "-c", " ".join(build_cmd)]

    if not dry_run:
        print()
        # Run subprocess command to build image
        result = run_subprocess(bash_cmd, source)

        # Check for errors
        if result != 0:
            raise RuntimeError(f"Build command failed with exit code {result}")

    if dry_run:
        print()
        print(f"Will build image {image!r}")
        print(f"Will use Dockerfile from {str(dockerfile)!r}")
        print(
            f"Will build image with UID {user_id}, GID {group_id}, and group name {group_name}"
        )
        print(f"Will build image with tag {image_path}")
        print("Will run the following bash command:")
        print(bash_cmd)

    return image_path


def tag_image(
    image_path: str,
    tags: list[str],
    dry_run: bool = False,
):
    # Construct list of tags to create
    base_path = image_path.split(":")[0]
    new_tags = [f"{base_path}:{tag}" for tag in tags]

    # Construct bash command to add all additional tags
    tag_cmd = [
        f"for IMAGE in {' '.join(new_tags)};",
        f"do podman tag {image_path} $IMAGE;",
        "done",
    ]
    bash_cmd = ["bash", "-c", " ".join(tag_cmd)]
    if not dry_run:
        print()
        # Run subprocess command to tag image
        result = run_subprocess(bash_cmd)

        # Check for errors
        if result != 0:
            raise RuntimeError(f"Tag command failed with exit code {result}")

    if dry_run:
        print()
        print("Will run the following bash command:")
        print(bash_cmd)
        for tag in new_tags:
            print(f"Will create new tag {tag}")

    return new_tags


def push_images(
    images: list[str],
    dry_run: bool = False,
):
    # Construct bash command to push images
    push_cmd = [f"for IMAGE in {' '.join(images)};", "do podman push $IMAGE;", "done"]
    bash_cmd = ["bash", "-c", " ".join(push_cmd)]
    if not dry_run:
        print()
        # Run subprocess command to push image
        result = run_subprocess(bash_cmd)

        # Check for errors
        if result != 0:
            raise RuntimeError(f"Push command failed with exit code {result}")

    if dry_run:
        print()
        print("Will run the following bash command:")
        print(bash_cmd)
        for image in images:
            print(f"Will push image {image}")

    return True


def cleanup(dry_run: bool = False):
    # Construct bash command to push images
    cleanup_cmd = [
        "podman image prune -f",
    ]
    bash_cmd = ["bash", "-c", " ".join(cleanup_cmd)]
    if not dry_run:
        print()
        # Run subprocess command to clean up Podman repo
        result = run_subprocess(bash_cmd)

        # Check for errors
        if result != 0:
            raise RuntimeError(f"Cleanup command failed with exit code {result}")

    if dry_run:
        print()
        print("Will run the following bash command:")
        print(bash_cmd)

    return True


def run():

    parser = ArgumentParser(
        description=(
            "Uses Podman to build, tag, and push the specified images either locally "
            "or to a remote repository"
        )
    )

    parser.add_argument(
        "images",
        nargs="+",
        help=("Space-separated list of Murfey Dockerfiles that you want to build."),
    )

    parser.add_argument(
        "--tags",
        "-t",
        nargs="*",
        default=["latest"],
        help=("Space-separated list of tags to apply to the built images"),
    )

    parser.add_argument(
        "--source",
        "-s",
        default=".",
        help=("Directory path to the Murfey repository"),
    )

    parser.add_argument(
        "--destination",
        "-d",
        default="localhost",
        help=("The URL of the repo to push the built images to"),
    )

    parser.add_argument(
        "--user-id",
        default=os.getuid(),
        help=("The user ID to install in the images"),
    )

    parser.add_argument(
        "--group-id",
        default=os.getgid(),
        help=("The group ID to install in the images"),
    )

    parser.add_argument(
        "--group-name",
        default=(
            grp.getgrgid(os.getgid()).gr_name if hasattr(grp, "getgrgid") else "nogroup"
        ),
        help=("The group name to install in the images"),
    )

    parser.add_argument(
        "--dry-run",
        default=False,
        action="store_true",
        help=(
            "When specified, prints out what the command would have done for each "
            "stage of the process"
        ),
    )

    args = parser.parse_args()

    # Validate the paths to the images
    for image in args.images:
        if not (Path(args.source) / "Dockerfiles" / str(image)).exists():
            raise FileNotFoundError(
                "No Dockerfile found in "
                f"source repository {str(Path(args.source).resolve())!r} for"
                f"image {str(image)!r}"
            )

    # Build image
    images = []
    for image in args.images:
        image_path = build_image(
            image=image,
            tag=args.tags[0],
            source=args.source,
            destination=(
                str(args.destination).rstrip("/")
                if str(args.destination).endswith("/")
                else str(args.destination)
            ),
            user_id=args.user_id,
            group_id=args.group_id,
            group_name=args.group_name,
            dry_run=args.dry_run,
        )
        images.append(image_path)

        # Create additional tags (if any) for each built image
        if len(args.tags) > 1:
            new_tags = tag_image(
                image_path=image_path,
                tags=args.tags[1:],
                dry_run=args.dry_run,
            )
            images.extend(new_tags)

    # Push all built images to specified repo
    push_images(images, dry_run=args.dry_run)

    # Perform final cleanup
    cleanup(dry_run=args.dry_run)

    # Notify that job is completed
    print()
    print("Done")


# Allow it to be run directly from the file
if __name__ == "__main__":
    run()
