from pathlib import Path
import dagfactory

yaml_dir = Path(__file__).parent / "yaml_dags"

for yaml_file in yaml_dir.glob("*.yaml"):
    dagfactory.load_yaml_dags(globals_dict=globals(), config_filepath=str(yaml_file))