import os
import shutil
import logging

from dataflows import Flow
from dataflows.processors.dumpers.file_dumper import FileDumper
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.specs import pipelines
from datapackage_pipelines.utilities.flow_utils import spew_flow
from datapackage_pipelines.utilities.stat_utils import STATS_DPP_KEY, STATS_OUT_DP_URL_KEY

logging.basicConfig(
    level=logging.WARNING,
)
logger = logging.getLogger(__name__)

class PathDumper(FileDumper):

    def __init__(self, out_path='.', **options):
        super(PathDumper, self).__init__(options)
        self.out_path = out_path
        self.save_pipeline_spec = options.get('save_pipeline_spec', False)
        PathDumper.__makedirs(self.out_path)

    def write_file_to_output(self, filename, path):
        path = os.path.join(self.out_path, path)
        # Avoid rewriting existing files
        if self.add_filehash_to_path and os.path.exists(path):
            return
        path_part = os.path.dirname(path)
        PathDumper.__makedirs(path_part)
        shutil.copy(filename, path)
        # Change file permissions to 777
        os.chmod(path, 0o775)
        return path

    def handle_datapackage(self):
        '''
        WARNING:
            save_pipeline_spec is hacky and might not always work

            Processors have no way of knowing what the full procesosr list looks like,
            The workaround is that it seems like when you run DPP this file's working
            directory is the exact directory where the pipeline-spec.yaml lives. We can then
            somewhat blindly create the full path to the pipeline-spec.yaml and pass it to the
            wite_file_to_output function

        '''
        if self.save_pipeline_spec:
            path = os.path.realpath('./pipeline-spec.yaml')
            try:
                self.write_file_to_output(path, 'pipeline-spec.yaml')
            except Exception as e:
                logger.warn(
                    f'Failed to save the pipeline-spec.yaml: {str(e)}',
                )

        super(PathDumper, self).handle_datapackage()

    @staticmethod
    def __makedirs(path):
        os.makedirs(path, exist_ok=True)




def flow(parameters: dict, stats: dict):
    out_path = parameters.pop('out-path', '.')
    stats.setdefault(STATS_DPP_KEY, {})[STATS_OUT_DP_URL_KEY] = os.path.join(out_path, 'datapackage.json')
    return Flow(
        PathDumper(
            out_path,
            **parameters
        )
    )



if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters, ctx.stats), ctx)
