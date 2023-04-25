pipeline {
    agent any
    // agent {
    //     docker { 
    //         image "python:3.8"
    //         args '--user 0:0'
    //     }
    // }

    environment {
        PATH = "C:\\WINDOWS\\SYSTEM32"
    }

    stages {
        stage('Clean Workspace'){
            steps {
                cleanWs()
            }
        }

        stage ('Code Checkout Process'){
            steps{
                checkout scm
            }
        }

        stage('Build') {
            steps {
                bat "C:\\Users\\GauravTalele\\AppData\\Local\\Programs\\Python\\Python310\\python.exe --version"
                bat "C:\\Users\\GauravTalele\\AppData\\Local\\Programs\\Python\\Python310\\python.exe -m venv venv"
                bat "venv\\Scripts\\activate.bat"
                bat "C:\\Users\\GauravTalele\\AppData\\Local\\Programs\\Python\\Python310\\python.exe -m pip install --upgrade pip"
                bat "C:\\Users\\GauravTalele\\AppData\\Local\\Programs\\Python\\Python310\\Scripts\\pip.exe install -r requirements.txt"
            }
        }

        stage('Deploy Prefect Flow') {
            steps {
                 bat "prefect --version"
            }
        }

    }
}