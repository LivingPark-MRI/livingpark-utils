"""Generate the `run.py` file.

It creates convinience methods to evoke the scripts generated from notebooks.
"""
import pathlib
import os
from string import Template


notebooks_dir = pathlib.Path(__file__).parents[1].joinpath("notebooks").resolve()
scripts_dir = pathlib.Path(__file__).parents[1].joinpath("scripts").resolve()
output_file = pathlib.Path(__file__).parent.joinpath("run.py").resolve()

with open(output_file, "w") as fout:
    fout.write(
        """\"\"\"Convinience methods to run the auto-generated scripts.

Notes
-----
The methods below need to be created manually when a new notebook is added.
\"\"\"
import importlib
from IPython.utils import io
"""
    )

for root, dirs, files in os.walk(notebooks_dir):
    if root.endswith(".ipynb_checkpoints"):
        continue

    for filename in files:
        rel_path = pathlib.Path(root).joinpath(filename).relative_to(notebooks_dir)
        script_path = scripts_dir.joinpath(str(rel_path).removesuffix(".ipynb") + ".py")

        if os.path.exists(script_path):
            with open(script_path) as fin, open(output_file, "a") as fout:
                s = Template(
                    """

def $func_name():
    \"\"\"Execute auto-generated script for `../notebooks/${func_name}.ipynb`.\"\"\"
    with io.capture_output():
        importlib.import_module(f\"livingpark_utils.scripts.$func_name\")
"""
                )
                fout.write(
                    s.safe_substitute(
                        {
                            "func_name": str(rel_path).removesuffix(".ipynb"),
                        }
                    )
                )
