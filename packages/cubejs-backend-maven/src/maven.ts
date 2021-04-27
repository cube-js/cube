/* eslint-disable no-restricted-syntax,newline-per-chained-call */
import { create } from 'xmlbuilder2';
import * as fs from 'fs';
import * as os from 'os';
import path from 'path';
import { spawnSync } from 'child_process';
import { downloadAndExtractFile } from '@cubejs-backend/shared';

type MavenDependency = {
  groupId: string,
  artifactId: string,
  version: string,
};

type ResolveOptions = {
  showOutput: boolean,
};

const MINIMAL_VERSION = '3.6.3';
const RECOMMENDED_VERSION = '3.8.0';

export function generateXml(dependencies: MavenDependency[]) {
  const root = create()
    .ele('project', {
      xmlns: 'http://maven.apache.org/POM/4.0.0',
      'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
      'xsi:schemaLocation': 'http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd',
    })
    // project->
    .ele('modelVersion').txt('4.0.0').up()
    .ele('groupId').txt('com.mycompany.app').up()
    .ele('artifactId').txt('my-app').up()
    .ele('version').txt('1.0-SNAPSHOT').up()
    // // project->properties
    // .ele('properties')
    // .ele('maven.compiler.source').txt('1.8').up()
    // .ele('maven.compiler.target').txt('1.8').up()
    // .up()
    // project->dependencies
    .ele('dependencies');

  for (const dependency of dependencies) {
    // project->dependencies->dependency
    const depRoot = root.ele('dependency');

    for (const [key, value] of Object.entries(dependency)) {
      depRoot.ele(key).txt(value);
    }

    depRoot.up();
  }

  return root.up().end({ prettyPrint: true });
}

async function getSystemMavenVersion() {
  const mvnOutput = spawnSync('mvn', ['--version'], {
    stdio: 'pipe',
    encoding: 'utf8'
  });
  if (mvnOutput.status === 0) {
    const result = mvnOutput.stdout.match(/Apache Maven (\d+).(\d+).(\d+)/);
    if (result) {
      return {
        binary: 'mvn',
        version: [
          parseInt(result[1], 10),
          parseInt(result[2], 10),
          parseInt(result[3], 10),
        ]
      };
    }
  }

  return null;
}

async function getExternalMaven() {
  const cwd = path.join(process.cwd(), 'download');
  const binary = path.join(cwd, `apache-maven-${RECOMMENDED_VERSION}`, 'bin', 'mvn');

  if (fs.existsSync(binary)) {
    return {
      binary,
      version: [RECOMMENDED_VERSION.split('.')]
    };
  }

  await downloadAndExtractFile(
    `https://apache-mirror.rbc.ru/pub/apache/maven/maven-3/3.8.0/binaries/apache-maven-${RECOMMENDED_VERSION}-bin.tar.gz`,
    {
      cwd,
      showProgress: true,
    }
  );

  return {
    binary,
    version: [RECOMMENDED_VERSION.split('.')]
  };
}

async function getMaven(options: ResolveOptions) {
  const systemBinary = await getSystemMavenVersion();
  if (systemBinary) {
    const [major, minor, patch] = systemBinary.version;
    const [minMajor, minMinor, minPatch] = MINIMAL_VERSION.split('.').map((v) => parseInt(v, 10));

    if (major > minMajor) {
      return systemBinary;
    }

    if (major === minMajor) {
      if (minor >= minMinor) {
        return systemBinary;
      } else if (minor === minMinor && patch >= minPatch) {
        return systemBinary;
      }
    }
  }

  if (options.showOutput) {
    if (systemBinary) {
      console.log(
        `[maven-resolve] Unable to use mvn from the system, current: ${systemBinary.version.join('.')}, ` +
        `minimal version: ${MINIMAL_VERSION}`
      );
    } else {
      console.log(
        `[maven-resolve] Unable to find mvn from the system, trying to download ${RECOMMENDED_VERSION}`
      );
    }
  }

  return getExternalMaven();
}

async function installDependencies(pathToPom: string, options: ResolveOptions) {
  const { binary } = await getMaven(options);

  // https://search.maven.org/artifact/de.qaware.maven/go-offline-maven-plugin/1.2.8/jar
  const mvnOutput = spawnSync(binary, ['de.qaware.maven:go-offline-maven-plugin:1.2.8:resolve-dependencies', '-f', pathToPom], {
    stdio: options.showOutput ? 'inherit' : 'pipe',
    encoding: 'utf8'
  });
  if (mvnOutput.status === 0) {
    return;
  }

  throw new Error(
    `Unable to resolve maven dependencies, mvn exited with: "${mvnOutput.status}" code`
  );
}

export async function resolveDependencies(dependecies: MavenDependency[], options: ResolveOptions) {
  const pathToPom = path.resolve(os.tmpdir(), 'cubejs-pom.xml');

  fs.writeFileSync(pathToPom, generateXml(dependecies));

  await installDependencies(pathToPom, options);
}

export function getDependenciesFromPackage(): MavenDependency[] {
  const pathToLock = path.join(process.cwd(), 'package.json');

  if (!fs.existsSync(pathToLock)) {
    throw new Error(
      `Unable to find package.json file in current working directory: ${process.cwd()}`
    );
  }

  const content = fs.readFileSync(pathToLock, {
    encoding: 'utf-8'
  });

  const pkg = JSON.parse(content);
  if (pkg) {
    if (pkg.java && pkg.java.dependencies) {
      return pkg.java.dependencies;
    }
  }

  return [];
}
