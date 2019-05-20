# bcodmo_processors
Custom datapackage-pipelines processors for BCODMO

To run the dpp command locally using the custom processors located in this repository, simply clone this reposistory and add the environment variable DPP_PROCESSOR_PATH.
If this repository is located at $PROCESSOR_REPO, the environment variable will be $GENERATOR_REPO/bcodmo_processors.
If you want to get rid of the bcodmo_pipeline_processors prefix you can instead set DPP_PROCESSOR_PATH to $GENERATOR_REPO/bcodmo_processors/bcodmo_pipeline_processors.
You can add environment variables manually using export DPP_PROCESSOR_PATH=$PUT_PATH_HERE or you can place all of your environment variables in a .env file and run the following commands:

```
set -a
source .env
```

Now when using dpp it will first look inside this repository when resolving processors.
