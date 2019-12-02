# Code to host flask application

#import funtions
import os
import subprocess
from flask import Flask,request
from flask import Flask, render_template      
import test

#create flask app instance
app = Flask(__name__)

#img folder to store images for display in app
IMG_FOLDER = os.path.join('static', 'img')
app.config['UPLOAD_FOLDER'] = IMG_FOLDER


@app.route("/", methods=["GET", "POST"])
def home():
    if request.method == "POST":

        # On submit run script to get the forecasted weather data for given Zipcode and date    
        airport = request.form['tbAirportName']
        zip = int(request.form['tbZipcode'])
        date = request.form['tbDate']
        dist = int(request.form['tbDist'])
        
        subprocess.check_output("python test.py "+str(zip)+" "+str(date)+" "+str(dist)+" "+str(airport),shell=True)   
 
        # wait for user input. Finish calculations on HPC cluster and wait till result is available  
        input("")

        subprocess.check_output("scp asb862@dumbo.hpc.nyu.edu:src/res.txt res.txt",shell=True)       
        file = open("res.txt","r")
        res=file.read()
       
        # if res is 1 that is Prediction is flight will be delayed 
        if(res):
           return '''
                <html>
                    <body>
                        <p>"There is chance of flight getting delayed"</p>
                        <p><a href="/">Click here to check again</a>
                    </body>
                </html>
           '''
        else:
           return '''
                <html>
                    <body>
                        <p>"Flight will depart on scheduled time"</p>
                        <p><a href="/">Click here to check again</a>
                    </body>
                </html>
           '''

    # Render home page            
    full_filename = os.path.join(app.config['UPLOAD_FOLDER'], 'flight.png')   
    return render_template("home.html", user_image=full_filename)   
	
 
if __name__ == "__main__":
    #run app on localhost
    app.run(debug=True)