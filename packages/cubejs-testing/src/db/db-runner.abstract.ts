export interface DBRunnerContainerOptions {
  volumes: {
    source: string,
    target: string,
    bindMode?: 'rw' | 'ro'
  }[]
}

// @todo Finish with old one PR for it
export abstract class DbRunnerAbstract {

}
