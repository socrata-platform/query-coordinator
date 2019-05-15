@Library('socrata-pipeline-library')

/* ========[ Converting your sbt-build job call into a pipeline ]=======
This is a list of the parameters passed into an sbt-build job call and where to use them in this pipeline:

REPO_URL - The git repo url will be part of the multi-branch pipeline configuration in jenkins
BRANCH_SPECIFIER - The branch/sha to build will come from the github hook automatically
BUILD_ASSEMBLY - If your project set this to false, specify a non-assembly build type (see BUILD_TYPE below)
PUSH_DOCKER_IMAGE - If set to false, make sure stage_dockerize is also set to false 
DEPLOY_TO_STAGING - May change whether stage_deploy is set true or false
PROJECT - Set the 'service' variable to this value
PROJECT_WD - Set the 'project_wd' variable to this value
RUN_COVERAGE - Call sbtbuild.setRunCoverage(true) in the Build stage
OVERRIDE_TEST_COMMAND - Call sbtbuild.setTestCommand(<test command>) in the Build stage
OVERRIDE_DOCKER_ROOT - Override the path being used in the docker_build() call in the Dockerize stage
OVERRIDE_SERVICE_NAME_PATTERN - Set the 'deploy_service_pattern' to this value
SCALA_VERSION - Call sbtbuild.setScalaVersion(<version>) in the Build stage
BUILD_TYPE - Call sbtbuild.setBuildType(<type>) in the Build stage
OVERRIDE_SRC_JAR - Call sbtbuild.setSrcJar(<value>) in the Build stage
OVERRIDE_SERVICE_VERSION - Override the service version used in the docker_build() call in the Dockerize stage
DEPLOY_TO_CHRONOS - See Chronos Deploy section in the Deploy stage
RUN_CHRONOS_TASK - See the Chronos Deploy section in the Deploy stage
SBT_CUSTOM_INIT - Call sbtbuild.setCustomInit(<value>) in the Build stage
CROSSCOMPILE - If false, call sbtbuild.setCrossCompile(false) in the Build stage
PUBLISH - If true, call sbtbuild.setPublish(true) in the Build stage (only works with "library" build type)
MARATHON_DEPLOY_ENVIRONMENT - Set the 'deploy_environment' to this value (only works for "staging" and "rc")
NO_SUBPROJECT_FOR_SBT - If true, call sbtbuild.setNoSubproject(true) in the Build stage
RUN_SBT_IT_TEST - If true, call sbtbuild.setRunITTest(true) in the Build stage

Note:  If your project has multiple sbt-build calls, you will need multiple clones of the SBTBuild library object
*/

def service = "query-coordinator" // This will be the PROJECT parameter in the sbt-build call for your project
def project_wd = "query-coordinator" // This will be the PROJECT_WD parameter in the sbt-build call
def deploy_service_pattern = "query-coordinator" // This will be the OVERRIDE_SERVICE_NAME_PATTERN in the sbt-build call (or likely the same as 'service' otherwise)
def deploy_environment = "staging" // master builds default to staging deploy, if cutting to rc this will be changed in the "Cut" stage below

def service_sha = env.GIT_COMMIT

// variables that determine which stages we run based on what triggered the job
def stage_cut = false
def stage_build = false
def stage_test = false
def stage_dockerize = false
def stage_deploy = false

// instanciate libraries
def sbtbuild = new com.socrata.SBTBuild(steps, service, project_wd)
def dockerize = new com.socrata.Dockerize(steps, service, BUILD_NUMBER)
def deploy = new com.socrata.MarathonDeploy(steps)

pipeline {
  options {
    ansiColor('xterm')
  }
  agent {
    label 'build-worker'
  }
  environment {
    PATH = "${WORKER_PATH}"
  }

  stages {
    stage('Setup') {
      steps {
        script {
          // checkout the repo with whatever branch/sha provided by the github hook
          checkout scm

          // determine what triggered the build and what stages need to be run

          if (params.RELEASE_CUT == true) { // RELEASE_CUT parameter was set by a cut job
            stage_cut = true  // other stages will be turned on in the cut step as needed
            deploy_environment = "rc"
          }
          else if (env.CHANGE_ID != null) { // we're running a PR builder
            stage_build = true
            // Query-coordinator just uses sbt clean/compile/test for testing (these are the default sbt build operations)
            // If your service has additional testing, you may want to use the Test stage to implement that and uncomment the next line:
            // stage_test = true
          }
          else if (BRANCH_NAME == "master") { // we're running a build on master branch to deploy to staging
            stage_build = true
            stage_dockerize = true
            stage_deploy = true
          }
          else {
            // we're not sure what we're doing...
            echo "Unknown build trigger - Exiting as Failure"
            currentBuild.result = 'FAILURE'
            return
          }
        }
      }
    }
    // The Cut stage is triggered when making a release candidate and will require a 2nd pipeline job that uses this Jenkinsfile
    // with an added RELEASE_CUT parameter
    // If the service doesn't have a formal cut or release candidate process, you may remove this stage
    stage('Cut') {
      when { expression { return stage_cut == true } }
      steps {
        script {
          def cutNeeded = false

          // get a list of all files changes since the last tag
          files = sh(returnStdout: true, script: "git diff --name-only HEAD `git describe --match \"v*\" --abbrev=0`").trim()
          echo "Files changed:\n${files}"

          if (files == 'version.sbt') {
            // Build anyway using latest tag - needed if sbt release had to be run between cuts
            // This parameter will need to be set by the cut job in Jenkins
            if(params.FORCE_BUILD) {
              cutNeeded = true
            }
            else {
              echo "No build needed, skipping subsequent steps"
            }
          }
          else {
            echo 'Running sbt-release'

            // The git config setup required for your project prior to running 'sbt release with-defaults' may vary:
            sh(returnStdout: true, script: "git config remote.origin.fetch +refs/heads/*:refs/remotes/origin/*")
            sh(returnStdout: true, script: "git config branch.master.remote origin")
            sh(returnStdout: true, script: "git config branch.master.merge refs/heads/master")

            echo sh(returnStdout: true, script: "echo y | sbt \"release with-defaults\"")

            cutNeeded = true
          }

          if(cutNeeded == true) {
            echo 'Getting release tag'
            release_tag = sh(returnStdout: true, script: "git describe --abbrev=0 --match \"v*\"").trim()
            branchSpecifier = "refs/tags/${release_tag}"
            echo branchSpecifier

            // checkout the tag so we're performing subsequent actions on it
            sh "git checkout ${branchSpecifier}"

            // set the service_sha to the current tag because it might not be the same as env.GIT_COMMIT
            service_sha = sh(returnStdout: true, script: "git rev-parse HEAD").trim()

            // set later stages to run since we're cutting
            stage_build = true
            stage_dockerize = true
            stage_deploy = true            
          }
        }
      }
    }
    stage('Build') {
      when { expression { return stage_build == true } }
      steps {
        script {
          // perform any needed modifiers on the build parameters here

          // If your project has a BUILD_TYPE parameter or sets BUILD_ASSEMBLY to false in the sbt-build call:
          // Set the build type if not performing an "assembly" build which produces a dockerized service
          // The following build types are supported:  assembly, library, native-packager, test
          // sbtbuild.setBuildType(build_type)

          // set a different execution command (not common!)
          // sbtbuild.setCustomCommand(cmd)

          // If your project has a SCALA_VERSION parameter in the sbt-build call:
          // set the version of scala used by the project (default is 2.10)
          // sbtbuild.setScalaVersion("2.11")

          // If your project has a OVERRIDE_TEST_COMMAND parameter in the sbt-build call:
          // set a custom test command (default is 'test')
          // sbtbuild.setTestCommand(test_command)

          // If your project has a SBT_CUSTOM_INIT parameter in the sbt-build call:
          // set custom initilization commands when running sbt clean/compile/test
          // sbtbuild.setCustomInit(custom_init)

          // set an optional coverage command when running sbt clean/compile/test (default is '+coverage')
          // sbtbuild.setCoverageCommand(coverage_command)

          // If your project sets the PUBLISH parameter to true in the sbt-build call:
          // set whether or not to publish a 'library' build_type (default is false)
          // sbtbuild.setPublish(true)

          // If your project sets RUN_COVERAGE to true in the sbt-build call:
          // set to run +coverage along with the sbt clean/compile/test commands
          // sbtbuild.setRunCoverage(true)

          // If your project sets the CROSSCOMPILE parameter to false in the sbt-build call:
          // set to use the '+' prefix for cross-compilation on sbt commands (default is true)
          // sbtbuild.setCrossCompile(false)

          // If your project sets the RUN_SBT_IT_TEST parameter to true in the sbt-build call:
          // set to run optional 'it:test' command (default is false)
          // sbtbuild.setRunITTest(true)

          // If your project sets the NO_SUBPROJECT_FOR_SBT parameter to true in the sbt-build call:
          // set whether or not to use a subprojecrt folder to run sbt commands (default is false)
          // sbtbuild.setNoSubproject(no_subproject)

          // If your project sets has a OVERRIDE_SRC_JAR parameter in the sbt-build call:
          // set when sbt configuration builds a custom jar location
          // sbtbuild.setSrcJar(src_jar)

          // set when using a custom target artifact jar name (not common!)
          // sbtbuild.setJarBasename(jar_basename)


          // build
          echo "Building sbt project..."
          sbtbuild.build()
        }
      }
    }
    stage('Test') {
      when { expression { return stage_test == true } }
      steps {
        script {
          echo "Additional testing steps happen here!"
        }
      }
    }
    stage('Dockerize') {
      when { expression { return stage_dockerize == true } }
      steps {
        script {
          // perform any needed modifiers on the dockerize parameters here

          // pass in custom build arguments used when building the docker container (not common)
          // dockerize.setBuildArgs(docker_build_args)

          // instructs docker_build() to use the native sbt:docker:publishLocal packager command
          // dockerize.setUseNativePackager()

          // Set to false if you do not want dockerize to copy build artifacts to the docker folder when building the docker container
          // This is not common and you can also pass a blank string into the docker_artifacts argument in docker_build()
          // dockerize.setCopyArtifacts(copy_artifacts)

          // Build the docker container
          // docker_build(service_version, build_sha, docker_path, docker_artifacts [, registry_push=true] )
          // service_version:  the sbt or semver used by the service to construct the docker image name
          // service_sha:      the sha used to build the service, also used to construct the docker image name
          // docker_path:      the path to where the Dockerfile lives in the project repo (default is ./<project_wd>/docker and is set by SBTBuild
          // docker_artifacts: a comma separated list of artifacts to build into the docker container - SBTBuild provides a default list
          // registry_push:    (Optional argument) - set to false if you do NOT want docker images pushed to internal, EU, and Fedramp registries
          //                                         NOTE:  Not pushing to registries will make apps-marathon deployments fail
          echo "Building docker container..."
          dockerize.docker_build(sbtbuild.getServiceVersion(), service_sha, sbtbuild.getDockerPath(), sbtbuild.getDockerArtifact())
        }
      }
    }
    stage('Deploy') {
      when { expression { return stage_deploy == true } }
      steps {
        script {
          // Checkout and run bundle install in the apps-marathon repo
          deploy.checkoutAndInstall()

          // deploy the service to the specified environment
          deploy.deploy(deploy_service_pattern, deploy_environment, dockerize.getDeployTag())
        }
      }
    }
  }
}
