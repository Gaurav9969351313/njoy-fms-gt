pipeline {
    agent {
        docker { 
            image "python:3.8"
            args '--user 0:0'
        }
    }

    stages {
        stage('Clean Workspace'){
            steps {
                cleanWs()
            }
        }

        stage('Initialize'){
             steps{
                def dockerHome = tool 'myDocker'
                env.PATH = "${dockerHome}/bin:${env.PATH}"
             }
        }

        stage ('checkout'){
            steps{
                checkout scm
            }
        }

        stage('Environment preparation') {
            steps {
                echo "-=- preparing project environment -=-"
                // Python dependencies
                sh "pip install -r requirements.txt"
            }
        }
    }
}