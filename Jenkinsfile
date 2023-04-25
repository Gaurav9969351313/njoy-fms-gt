pipeline {
    agent any
    // agent {
    //     docker { 
    //         image "python:3.8"
    //         args '--user 0:0'
    //     }
    // }

    stages {
        stage('Clean Workspace'){
            steps {
                cleanWs()
            }
        }

        stage ('checkout'){
            steps{
                checkout scm
            }
        }

        stage('Build') {
            steps {
                sh 'python -m venv env'
                sh '. env/bin/activate'
                sh 'pip install -r requirements.txt'
                sh 'python setup.py install'
            }
        }
    }
}