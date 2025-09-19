import praw
import json

with open(r"./secrets/secrets.json", "r") as f:
    secrets = json.load(f)

if __name__ == '__main__':
    print(secrets)
    reddit = praw.Reddit(
        client_id=secrets["client_id"],
        client_secret=secrets["client_secret"],
        user_agent="AlphaPrism",
    )

    print(reddit.user.me())
    print(reddit.auth.scopes())
    print(reddit.read_only)

    for submission in reddit.subreddit("wallstreetbets").hot(limit=10):
        print(submission.title)
        # Output: the submission's title
        print(submission.score)
        # Output: the submission's score
        print(submission.id)
        # Output: the submission's ID
        print(submission.url)

        print(submission.comments)

        print(submission.selftext)

    WSB = reddit.subreddit("wallstreetbets")
    WSB.display_name