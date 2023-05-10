export interface ContainerVolumeDefinition {
  source: string,
  target: string,
  bindMode?: 'rw' | 'ro'
}

export interface DBRunnerContainerOptions {
  volumes?: ContainerVolumeDefinition[]
}

// @todo Finish with old one PR for it
export abstract class DbRunnerAbstract {

}
