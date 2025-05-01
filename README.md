# No effort flake chaser

This is a little script to run a program many times until a certain regex match is found.

Imagine you have a test that often works but sometimes it doesn't, like `fake_flake.py`, and need to debug it. Instead of running it manually many times until one reproduces the issue you're interested in, you can use `flake_chaser.py` to do that for you.

Furthermore, `flake_chaser.py` can run many instances at the same time, which not only reduces the time to get a lucky run, but also can also help randomize scheduling and therefore the chances of unusual race conditions showing up.

## Lack of polish

At this point there is no command line interface. Instead, you have to edit `flake_chaser.py`.

I haven't been as diligent with expection handling as I could.

It's also the first time I use asyncio for subprocess handling, and I'm still relatively unfamiliar with Python's asyncio.

This is just a script written in a hurry that served me once.