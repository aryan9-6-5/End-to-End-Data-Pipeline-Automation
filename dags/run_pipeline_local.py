from tasks.load_data import load_data
from tasks.validate_data import validate_data
from tasks.heal_data import heal_data
from tasks.transform_data import transform_data

if __name__ == "__main__":
    print(" Starting local pipeline run...\n")
    load_data()
    validate_data()
    heal_data()
    transform_data()
    print("\n Local pipeline run completed.")
