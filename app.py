from flask import Flask, render_template, flash, redirect, url_for, session, request, current_app, send_from_directory
from wtforms import Form, SelectField, StringField, TextAreaField, validators
from werkzeug.utils import secure_filename

from csv_generator import generateCSV

import os
app = Flask(__name__)
app._static_folder = os.path.abspath("static")
UPLOAD_FOLDER = './uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER 


class ScrapeForm(Form):
    media_id = SelectField(u'Media', choices=[('Nacion', 'La Nación'), 
                                            ('CRHoy', 'CrHoy'), 
                                            ('Financiero', 'Financiero'),
                                            ('Semanario', 'Semanario'),
                                            ('Tico Times', 'Tico Time'),
                                            ('Extra', 'La Extra')])
    since_date = StringField('Since date', [validators.Length(min=17, max=17)])
    until_date = StringField('Until Date', [validators.Length(min=17, max=17)])

@app.route('/', methods=['GET', 'POST'])
def index():
    form = ScrapeForm(request.form)
    if request.method == 'POST':
        mID = form.media_id.data
        sDt = form.since_date.data
        uDt = form.until_date.data
        fileName = generateCSV(mID, sDt, uDt)
        uploads = os.path.join(current_app.root_path, app.config['UPLOAD_FOLDER'])
        return send_from_directory(directory=uploads, filename=fileName)
    return render_template('index.html', form=form)

@app.route('/about')
def about():
    return render_template('about.html')

if __name__ == '__main__':
    app.run()
