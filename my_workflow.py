import time
import random
from prefect import flow, task


@flow
def show_stars(github_repos: list[str]):
    """Flow: Show the number of stars that GitHub repos have"""
    print("Starting the flow")
    probabilistic_sleep()

    for repo in github_repos:
        # Call Task 1
        repo_stats = fetch_stats(repo)

        # Call Task 2
        stars = get_stars(repo_stats)

        # Print the result
        print(f"{repo}: {stars} stars")


@task()
def probabilistic_sleep():
    rand_num = random.random() * 1000

    if rand_num < 990:
        sleep_duration = 0
    elif rand_num < 995:
        sleep_duration = 10
    elif rand_num < 999:
        sleep_duration = 30
    else:
        sleep_duration = 70

    print(f"Sleeping for {sleep_duration} seconds...")
    time.sleep(sleep_duration)
    print("Done sleeping!")


@task
def fetch_stats(github_repo: str):
    """Task 1: Fetch the statistics for a GitHub repo"""
    print(f"Fetching statistics for {github_repo}")
    return {}


@task
def get_stars(repo_stats: dict):
    """Task 2: Get the number of stars from GitHub repo statistics"""
    print("Returning statistics for GitHub repo")
    return 5


# Run the flow
if __name__ == "__main__":
    show_stars([
        "PrefectHQ/prefect",
        "pydantic/pydantic",
        "huggingface/transformers"
    ])
