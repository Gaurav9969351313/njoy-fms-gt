pipeline {
    agent any
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

        stage('Install Dependencies') {
            steps {
                sh 'pip install -r requirements.txt'
            }
        }
    }
}