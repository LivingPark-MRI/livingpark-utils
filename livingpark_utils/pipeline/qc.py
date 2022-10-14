"""Collection of Quality Control utilities."""
import glob
import os.path

from PIL import Image


def make_gif(frame_folder: str, output_name: str = "animation.gif") -> None:
    """Make gifs from a set of images located in the same folder.

    Parameters
    ----------
    frame_folder : str
        Folder where frames are stored. Frames must be in a format supported by PIL.

    output_name : str
        Base name of the gif file. Will be written in frame_folder.
    """
    frames = [
        Image.open(image)
        for image in glob.glob(os.path.join(f"{frame_folder}", "*.png"))
    ]
    frame_one = frames[0]
    frame_one.save(
        os.path.join(frame_folder, output_name),
        format="GIF",
        append_images=frames,
        save_all=True,
        duration=1000,
        loop=0,
    )
    print(f"Wrote {os.path.join(frame_folder, output_name)}")
