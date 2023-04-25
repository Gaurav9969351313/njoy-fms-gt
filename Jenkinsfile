pipeline {
    agent {
        docker {
            image 'python:3.7'
        }
    }

    environment {
        EMAIL_TO='gaurav.talele@indigochart.com'
    }

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

        stage('Environment preparation') {
            steps {
                echo "-=- preparing project environment -=-"
                // Python dependencies
                sh "pip install -r requirements.txt"
            }
        }
    }

    post {	
        always {  	
            echo 'This will always run'
        }	

        success {  	
             emailext body: 'Check console output at $BUILD_URL to view the results. \n\n ${CHANGES} \n\n -------------------------------------------------- \n${BUILD_LOG, maxLines=100, escapeHtml=false}', 	
                    to: 'gaurav.talele@indigochart.com', 	
                    subject: 'Build Sucessful in Jenkins: $PROJECT_NAME - #$BUILD_NUMBER. Docker production images sent to docker hub'	
        }	

        failure {	
            emailext body: 'Check console output at $BUILD_URL to view the results. \n\n ${CHANGES} \n\n -------------------------------------------------- \n${BUILD_LOG, maxLines=100, escapeHtml=false}', 	
                    to: 'gaurav.talele@indigochart.com', 	
                    subject: 'Build failed in Jenkins: $PROJECT_NAME - #$BUILD_NUMBER'	
        }	

        unstable {	
            emailext body: 'Check console output at $BUILD_URL to view the results. \n\n ${CHANGES} \n\n -------------------------------------------------- \n${BUILD_LOG, maxLines=100, escapeHtml=false}', 	
                    to: 'gaurav.talele@indigochart.com', 	
                    subject: 'Unstable build in Jenkins: $PROJECT_NAME - #$BUILD_NUMBER'	
        }	

        changed {	
            emailext body: 'Check console output at $BUILD_URL to view the results.', 	
                    to: 'gaurav.talele@indigochart.com', 	
                    subject: 'Jenkins build is back to normal: $PROJECT_NAME - #$BUILD_NUMBER'	
        }	
    }	
}