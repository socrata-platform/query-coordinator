@Library('socrata-pipeline-library')

def deploy_environment = "staging"
def service_sha = env.GIT_COMMIT

// Stage control variables to simplify stage selection by build cause
def stage_cut = false
def stage_build = false
def stage_dockerize = false
def stage_deploy = false

// Utility Libraries
def sbtbuild = new com.socrata.SBTBuild(steps, "query-coordinator", "query-coordinator")
def dockerize = new com.socrata.Dockerize(steps, "query-coordinator", BUILD_NUMBER)
def deploy = new com.socrata.MarathonDeploy(steps)

pipeline {
  options {
    ansiColor('xterm')
  }
  parameters {
    booleanParam(name: 'RELEASE_CUT', defaultValue: false, description: 'Are we cutting a new release candidate?')
    booleanParam(name: 'FORCE_BUILD', defaultValue: false, description: 'Force build from latest tag if sbt release needed to be run between cuts')
    string(name: 'AGENT', defaultValue: 'build-worker', description: 'Which build agent to use?')
  }
  agent {
    label params.AGENT
  }
  environment {
    PATH = "${WORKER_PATH}"
  }

  stages {
    stage('Setup') {
      steps {
        script {
          // checkout the repo
          checkout scm

          // determine what triggered the build and what stages need to be run
          if (params.RELEASE_CUT == true) { // we're running a release cut
            stage_cut = true  // other stages will be turned on in the cut stage if needed
            deploy_environment = "rc"
          }
          else if (env.CHANGE_ID != null) { // we're running a PR test
            stage_build = true
          }
          else if (BRANCH_NAME == "master") { // we're doing a master branch build for integration deploy
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
    stage('Cut') {
      when { expression { return stage_cut == true } }
      steps {
        script {
          def cutNeeded = false

          // get a list of all files changes since the last tag
          files = sh(returnStdout: true, script: "git diff --name-only HEAD `git describe --match \"v*\" --abbrev=0`").trim()
          echo "Files changed:\n${files}"

          if (files == 'version.sbt') {
            if(params.FORCE_BUILD) {
              // Build anyway using latest tag - needed if sbt release had to be run between cuts
              cutNeeded = true
            }
            else {
              echo "Version change only, no cut needed"
            }
          }
          else {
            echo 'Running sbt release'

            // sbt release doesn't like running without these
            sh(returnStdout: true, script: "git config remote.origin.fetch +refs/heads/*:refs/remotes/origin/*")
            sh(returnStdout: true, script: "git config branch.master.remote origin")
            sh(returnStdout: true, script: "git config branch.master.merge refs/heads/master")

            echo "Would run sbt \"release with defaults\" here - disabled for debugging"
            //echo sh(returnStdout: true, script: "echo y | sbt \"release with-defaults\"")

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

            // set stages to run since we're cutting
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
          echo "Building sbt project..."
          sbtbuild.build()
        }
      }
    }
    stage('Dockerize') {
      when { expression { return stage_dockerize == true } }
      steps {
        script {
          echo "Building docker container..."
          dockerize.docker_build(sbtbuild.getServiceVersion(), service_sha, sbtbuild.getDockerPath(), sbtbuild.getDockerArtifact())
        }
      }
    }
    stage('Deploy') {
      when { expression { return stage_deploy == true } }
      steps {
        script {
          // Checkout and run bundle install in the apps-marathon repo so we're running the latest config
          deploy.checkoutAndInstall()

          // deploy the service to the specified environment
          echo "Would deploy query-coordinator to ${deploy_environment} with image ${dockerize.getDeployTag()} here - disabled for debugging"
          //deploy.deploy("query-coordinator", deploy_environment, dockerize.getDeployTag())
        }
      }
    }
  }
}
