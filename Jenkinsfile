@Library('socrata-pipeline-library')

def service = 'query-coordinator'
def project_wd = 'query-coordinator'
def isPr = env.CHANGE_ID != null;

// Utility Libraries
def sbtbuild = new com.socrata.SBTBuild(steps, service, project_wd)
def dockerize = new com.socrata.Dockerize(steps, service, BUILD_NUMBER)

pipeline {
  options {
    ansiColor('xterm')
  }
  parameters {
    booleanParam(name: 'RELEASE_BUILD', defaultValue: false, description: 'Are we building a release candidate?')
    booleanParam(name: 'RELEASE_DRY_RUN', defaultValue: false, description: 'To test out the release build without creating a new tag.')
    string(name: 'AGENT', defaultValue: 'build-worker', description: 'Which build agent to use?')
    string(name: 'BRANCH_SPECIFIER', defaultValue: 'origin/main', description: 'Use this branch for building the artifact.')
  }
  agent {
    label params.AGENT
  }
  environment {
    SERVICE = 'query-coordinator'
  }
  stages {
    stage('Release Tag') {
      when {
        expression { return params.RELEASE_BUILD }
      }
      steps {
        script {
          if (params.RELEASE_DRY_RUN) {
            echo 'DRY RUN: Skipping release tag creation'
          }
          else {
            // get a list of all files changes since the last tag
            files = sh(returnStdout: true, script: "git diff --name-only HEAD `git describe --match \"v*\" --abbrev=0`").trim()
            echo "Files changed:\n${files}"

            // the release build process changes the version file, so it will always be changed
            // if there are other files changed, increment the version and create a new tag
            if (files != 'version.sbt') {
              echo 'Running sbt-release'

              // sbt release doesn't like running without these
              sh(returnStdout: true, script: "git config remote.origin.fetch +refs/heads/*:refs/remotes/origin/*")
              sh(returnStdout: true, script: "git config branch.main.remote origin")
              sh(returnStdout: true, script: "git config branch.main.merge refs/heads/main")

              echo sh(returnStdout: true, script: "echo y | sbt \"release with-defaults\"")
            }
          }
          echo 'Getting release tag'
          release_tag = sh(returnStdout: true, script: "git describe --abbrev=0 --match \"v*\"").trim()
          branchSpecifier = "refs/tags/${release_tag}"
          echo branchSpecifier

          // checkout the tag so we're performing subsequent actions on it
          sh "git checkout ${branchSpecifier}"
        }
      }
    }
    stage('Build') {
      steps {
        script {
          sbtbuild.setScalaVersion("2.12")
          sbtbuild.setSubprojectName("queryCoordinator")
          echo "Building sbt project..."
          sbtbuild.build()

          env.SERVICE_VERSION = sbtbuild.getServiceVersion()
          // set the SERVICE_SHA to the current head because it might not be the same as env.GIT_COMMIT
          env.SERVICE_SHA = sh(returnStdout: true, script: "git rev-parse HEAD").trim()
          // set build description to be the same as the docker deploy tag
          currentBuild.description = "${env.SERVICE}:${env.SERVICE_VERSION}_${env.BUILD_NUMBER}_${env.SERVICE_SHA.take(8)}"
        }
      }
    }
    stage('Dockerize') {
      when {
        not { expression { isPr } }
      }
      steps {
        script {
          echo "Building docker container..."
          echo sbtbuild.getDockerPath()
          echo sbtbuild.getDockerArtifact()
          dockerize.docker_build(sbtbuild.getServiceVersion(), env.SERVICE_SHA, sbtbuild.getDockerPath(), sbtbuild.getDockerArtifact())
          env.DOCKER_TAG = dockerize.getDeployTag()
        }
      }
      post {
        success {
          script {
            if (params.RELEASE_BUILD){
              echo env.DOCKER_TAG // For now, just print the deploy tag in the console output -- later, communicate to release metadata service
            }
          }
        }
      }
    }
    stage('Deploy') {
      when {
        not { expression { isPr } }
        not { expression { return params.RELEASE_BUILD } }
      }
      steps {
        script {
          // uses env.SERVICE and env.DOCKER_TAG, deploys to staging by default
          marathonDeploy()
        }
      }
    }
  }
}
