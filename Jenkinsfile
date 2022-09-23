@Library('socrata-pipeline-library')

def deploy_environment = "staging"
def service_sha = env.GIT_COMMIT
def default_branch_specifier = "origin/main"

def service = "query-coordinator"
def project_wd = "query-coordinator"
def deploy_service_pattern = "query-coordinator"

// Stage control variables to simplify stage selection by build cause
def boolean stage_cut = false
def boolean stage_build = false
def boolean stage_dockerize = false
def boolean stage_deploy = false

// Utility Libraries
def sbtbuild = new com.socrata.SBTBuild(steps, service, project_wd)
def dockerize = new com.socrata.Dockerize(steps, service, BUILD_NUMBER)
def deploy = new com.socrata.MarathonDeploy(steps)

pipeline {
  options {
    ansiColor('xterm')
  }
  parameters {
    booleanParam(name: 'RELEASE_CUT', defaultValue: false, description: 'Are we cutting a new release candidate?')
    string(name: 'AGENT', defaultValue: 'build-worker-pg13', description: 'Which build agent to use?')
    string(name: 'BRANCH_SPECIFIER', defaultValue: default_branch_specifier, description: 'Use this branch for building the artifact.')

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
          // check to see if we want to use a non-standard branch and check out the repo
          if (params.BRANCH_SPECIFIER == default_branch_specifier) {
            checkout scm
          } else {
            def scmRepoUrl = scm.getUserRemoteConfigs()[0].getUrl()
            checkout ([
              $class: 'GitSCM',
              branches: [[name: params.BRANCH_SPECIFIER ]],
              userRemoteConfigs: [[ url: scmRepoUrl ]]
            ])
          }

          // set the service sha to what was checked out (GIT_COMMIT isn't always set)
          service_sha = sh(returnStdout: true, script: "git rev-parse HEAD").trim()

          // determine what triggered the build and what stages need to be run
          if (params.RELEASE_CUT == true) { // we're running a release cut
            stage_cut = true  // other stages will be enabled in the cut stage if needed
            deploy_environment = "rc"
          }
          else if (env.CHANGE_ID != null) { // we're running a PR test
            stage_build = true
          }
          else if (BRANCH_NAME == "main") { // we're doing a main branch build for integration deploy
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
      when { expression { stage_cut } }
      steps {
        script {

          // get a list of all files changes since the last tag
          files = sh(returnStdout: true, script: "git diff --name-only HEAD `git describe --match \"v*\" --abbrev=0`").trim()
          echo "Files changed:\n${files}"

          if (files != 'version.sbt') {
            echo 'Running sbt release'

            // sbt release doesn't like running without these
            sh(returnStdout: true, script: "git config remote.origin.fetch +refs/heads/*:refs/remotes/origin/*")
            sh(returnStdout: true, script: "git config branch.main.remote origin")
            sh(returnStdout: true, script: "git config branch.main.merge refs/heads/main")

            echo sh(returnStdout: true, script: "echo y | sbt \"release with-defaults\"")
          }


          echo 'Getting release tag'
          release_tag = sh(returnStdout: true, script: "git describe --abbrev=0 --match \"v*\"").trim()
          branchSpecifier = "refs/tags/${release_tag}"
          echo branchSpecifier

          // checkout the tag so we're performing subsequent actions on it
          sh "git checkout ${branchSpecifier}"

          // set the service_sha to the current tag because it might not be the same as what was checked out
          service_sha = sh(returnStdout: true, script: "git rev-parse HEAD").trim()

          // set stages to run since we're cutting
          stage_build = true
          stage_dockerize = true
          stage_deploy = true
        }
      }
    }
    stage('Build') {
      when { expression { stage_build } }
      steps {
        script {
          sbtbuild.setScalaVersion("2.12")
          sbtbuild.setSubprojectName("queryCoordinator")
          echo "Building sbt project..."
          sbtbuild.build()
        }
      }
    }
    stage('Dockerize') {
      when { expression { stage_dockerize } }
      steps {
        script {
          echo "Building docker container..."
          dockerize.docker_build(sbtbuild.getServiceVersion(), service_sha, sbtbuild.getDockerPath(), sbtbuild.getDockerArtifact())
        }
      }
    }
    stage('Deploy') {
      when { expression { stage_deploy } }
      steps {
        script {
          // Checkout and run bundle install in the apps-marathon repo so we're running the latest config
          deploy.checkoutAndInstall()

          // deploy the service to the specified environment
          deploy.deploy(deploy_service_pattern, deploy_environment, dockerize.getDeployTag())
        }
      }
    }
  }
}
