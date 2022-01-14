# -*- coding: utf-8 -*-
"""
Created on Mon Jul 13 12:17:33 2020

@author: GaganMehta
"""

import PySimpleGUI as sg
import subprocess
import hashlib
import os

sg.theme('LightGray1')


def Execute_script(cmd, stagingfile, location):
    try:
        # sp = subprocess.Popen([cmd, stagingfile, location], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        sp = subprocess.Popen([cmd, stagingfile, location], shell=True, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, stdin=subprocess.PIPE, close_fds=True)
        # sp = subprocess.Popen([cmd, stagingfile, location], shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        out, err = sp.communicate()
        if out:
            global created_report
            print(out.decode("utf-8"))
            created_report = out.decode("ascii")
        if err:
            print(err.decode("utf-8"))
    except:
        pass


def GUI_call():
    choices = ('All', 'Account', 'Card', 'People', 'Customer')

    # sg.theme('SandyBeach')
    # sg.change_look_and_feel('DefaultNoMoreNagging')

    layout = [[sg.Text(' ' * 80)],
              [sg.Text('\tConversion Internal Verification \t', font='Calibri 40', text_color='blue',
                       relief=sg.RELIEF_RIDGE)],
              [sg.Text('_' * 155)],
              [sg.Text(' ' * 80)],
              [sg.Text('You may choose to perform verification for all or for a specific staging file',
                       font='CourierBody 15', text_color='black')],
              [sg.Text('Please select the option from the given list', font='CourierBody 15', text_color='black')],
              [sg.Text(' ' * 80)],
              [sg.Listbox(choices, font='CourierBody 15', size=(95, len(choices)), key='-STAGINGFILE-')],
              [sg.Text(' ' * 80)],
              [sg.Text('Please select location of report folder', size=(30, 1), font='CourierBody 15',
                       text_color='black'), sg.Input(key='-FILELOC-'), sg.FolderBrowse(target='-FILELOC-')],
              [sg.Text(size=(40, 1), text_color='red', font='CourierBody 22', key='-OUTPUT-')],
              [sg.Text(size=(40, 1), text_color='red', font='CourierBody 22', key='-OUTPUT2-')],
              [sg.OK(), sg.Exit()]]

    window = sg.Window('Internal Verification Tool', layout)

    while True:  # Event Loop
        event, values = window.read(timeout=100)

        if event in (sg.WIN_CLOSED, 'Exit', 'None'):
            break
        if event == 'OK':
            # checks whether the dropdown value was selected or not also check whether the File location was provided or not
            if values['-STAGINGFILE-'] and values['-FILELOC-']:

                """ adding new check for location check
                """
                check_dir = os.path.isdir(values['-FILELOC-'])
                if check_dir == False:
                    window['-OUTPUT-'].update("Invalid folder location")
                    window.refresh()
                else:

                    response = sg.popup_yes_no(
                        "Do you want to proceed with Verification of {}".format(values['-STAGINGFILE-'][0]))
                    if response in (sg.WIN_CLOSED, 'No'):
                        window['-OUTPUT-'].update('')
                        window['-OUTPUT2-'].update('')
                        window.refresh()
                        continue
                    if response == 'Yes':
                        report_location = values['-FILELOC-']

                        window.refresh()

                        """
                        code = "SICARDVERF3.py"
                        """
                        pathnm = 'C:\\Users\\gaganmehta\\codes'

                        if values['-STAGINGFILE-'][0] == 'Card':
                            codename = 'CARDVERF.py'
                            code = os.path.join(pathnm, codename)
                            Execute_script('python', code, report_location)
                        elif values['-STAGINGFILE-'][0] == 'Account':
                            codename = 'ACCTVERF.py'
                            code = os.path.join(pathnm, codename)
                            Execute_script('python', code, report_location)
                        elif values['-STAGINGFILE-'][0] == 'People':
                            codename = 'PPLVERF.py'
                            code = os.path.join(pathnm, codename)
                            Execute_script('python', code, report_location)
                        elif values['-STAGINGFILE-'][0] == 'Customer':
                            codename = 'CUSTVERF.py'
                            code = os.path.join(pathnm, codename)
                            Execute_script('python', code, report_location)
                        elif values['-STAGINGFILE-'][0] == 'All':
                            codename = 'CARDVERF.py'
                            code = os.path.join(pathnm, codename)
                            Execute_script('python', code, report_location)
                            codename = 'ACCTVERF.py'
                            code = os.path.join(pathnm, codename)
                            Execute_script('python', code, report_location)
                            codename = 'PPLVERF.py'
                            code = os.path.join(pathnm, codename)
                            Execute_script('python', code, report_location)
                            codename = 'CUSTVERF.py'
                            code = os.path.join(pathnm, codename)
                            Execute_script('python', code, report_location)

                        if values['-STAGINGFILE-'][0] == 'All':
                            window['-OUTPUT-'].update("All the reports are generated at the given location")
                            window['-OUTPUT2-'].update('')
                            window.refresh()
                        else:
                            window['-OUTPUT-'].update(
                                "Report is Generated for {} at the location".format(values['-STAGINGFILE-'][0]))
                            window['-OUTPUT2-'].update("File name is {}".format(created_report))
                            window.refresh()

                    more = sg.popup_yes_no("Do you wish to continue performing verification?")
                    if more in (sg.WIN_CLOSED, 'No'):
                        break
                    if more == 'Yes':
                        window.refresh()
                        window['-OUTPUT-'].update('')
                        window['-OUTPUT2-'].update('')
            else:
                if not values['-STAGINGFILE-']:
                    window['-OUTPUT-'].update("Please make a selection from the dropdown")
                    window.refresh()
                if not values['-FILELOC-']:
                    window['-OUTPUT-'].update("Please enter location of report")
                    window.refresh()

    window.close()


# Use this GUI to get your password's hash code
def HashGeneratorGUI():
    layout = [[sg.T('Password Hash Generator', size=(30, 1), font='Any 15')],
              [sg.T('Password'), sg.In(key='password')],
              [sg.T('SHA Hash'), sg.In('', size=(40, 1), key='hash')],
              ]

    window = sg.Window('SHA Generator', layout, auto_size_text=False, default_element_size=(10, 1),
                       text_justification='r', return_keyboard_events=True, grab_anywhere=False)

    while True:
        event, values = window.read()
        if event == sg.WIN_CLOSED:
            exit(69)

        password = values['password']
        try:
            password_utf = password.encode('utf-8')
            sha1hash = hashlib.sha1()
            sha1hash.update(password_utf)
            password_hash = sha1hash.hexdigest()
            window['hash'].update(password_hash)
        except:
            pass

        # determine if a password matches the secret password by comparing SHA1 hash codes


def PasswordMatches(password, hash):
    password_utf = password.encode('utf-8')
    sha1hash = hashlib.sha1()
    sha1hash.update(password_utf)
    password_hash = sha1hash.hexdigest()
    if password_hash == hash:
        return True
    else:
        return False


proceed_flag = False


def get_password():
    password = sg.popup_get_text('Password', password_char='*')
    if password == 'gui':
        # HashGeneratorGUI()
        GUI_call()
        return proceed_flag
        exit(69)
    # if password:
    #     if PasswordMatches(password, login_password_hash):
    #         print('Login SUCCESSFUL')
    #         proceed_flag = True
    #         GUI_call()
    #         return proceed_flag
    #     else:
    #         print('Login FAILED!!')
    #         sg.popup("You entered a wrong password. No worries! Try again!")
    #         proceed_flag = False
    #         get_password()


login_password_hash = '21faa67da1fdbfe3a5a17ee67b0e79f4017e8bfc'
get_password()