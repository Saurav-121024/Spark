import tkinter as tk
from tkinter import ttk
import tkcalendar
from tkcalendar import Calendar, DateEntry
# import mysql.connector
from copy import deepcopy
import psycopg2
import pandas as pd


# TKinter form

def design():
    global root
    main = ttk.Frame(root)
    main.pack(fill='both', expand=True)

    # establishing the connection
    conn1 = psycopg2.connect(user='postgres', password='Milestone@12', host='localhost', database='Prime')

    # Creating a cursor object using the cursor() method
    cursor = conn1.cursor()

    def submit():
        print(f'''{cal.get(), team_name_entry.get(), month_name.get(), discription_entry.get(), type_name.get(),
                   team_mem_entry.get(), project_entry.get(), environment_name.get(), defect_entry.get()
        }''')
        date = cal.get()
        team = team_name_entry.get()
        month = month_name.get()
        discription = discription_entry.get()
        type1 = type_name.get()
        team_member = team_mem_entry.get()
        project = project_entry.get()
        env = environment_name.get()
        defect = defect_entry.get()

        print(date, team, month, discription, type1, team_member, project, env, defect)
        sql = """INSERT INTO ACCOMP_SHEET(DATE_PICK, TEAM_NAME, MONTH_NAME, DISCRIPTION, ACCOMPLISHMENT_TYPE,
                 TEAM_MEM_NAMES, PROJECT_NAME, ENVIRONMENT, DEFECT_DETAIL)
                 VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)       
                 """
        data1 = (date, team, month, discription, type1, team_member, project, env, defect)
        cursor.execute(sql, data1)
        conn1.commit()

    # #     except:
    # #         conn.rollback()

    def quit_func():
        global root
        root.destroy()
        conn1.close()
        root = tk.Tk()
        root.title('Accomplishment Sheet')
        root.geometry("900x500")
        root.configure(background='pink')
        design()

    def reset():
        team_name_entry.delete(0, tk.END)
        month_name.set('January')
        discription_entry.delete(0, tk.END)
        type_name.set('Delivery Milestone')
        team_mem_entry.delete(0, tk.END)
        project_entry.delete(0, tk.END)
        environment_name.set('DEV')
        defect_entry.delete(0, tk.END)

    # create a function to display rows from the table

    def diplay_table():
        cursor.execute("SELECT * FROM ACCOMP_SHEET")
        global i
        i = 0
        for rows in cursor:
            for j in range(len(rows)):
                #                 e = ttk.Label(header_canvas, width=10, text=rows[j])
                #                 e.grid(row=i+17, column =j)
                e = ttk.Entry(header_canvas, width=15)
                e.grid(row=i + 1, column=j)
                e.insert(tk.END, rows[j])
            globals()[f'edit_button{i}'] = ttk.Button(header_canvas, text='edit row',
                                                      command=lambda k=rows[0]: edit_data(k))
            globals()[f'edit_button{i}'].grid(row=i + 1, column=11, sticky='w')
            i = i + 1

    def edit_data(id):
        print(id)
        global i
        sql2 = f"SELECT * FROM ACCOMP_SHEET WHERE id = {id}"
        table_row = cursor.execute(sql2)
        #         table_row = cursor.execute(sql2, (id,))
        print(table_row)
        s = cursor.fetchone()
        #         data_index = {}
        # .set_index(['id','date','team','month','disc','acc_type','mem','proj','env','def'])
        df1 = pd.DataFrame(s)
        print(df1)
        #         print(df1.columns)
        print(df1[0][0])
        abcd = int(df1[0][0])
        print(type(abcd))
        print(f'id: {df1.loc(0)}')
        #         print(s)

        #         for s1 in s:
        #             print(s1)
        # #             a = deepcopy(s1)
        #             print(type(str(s1)))

        #         print(type(s))
        e1_var_id = tk.StringVar(header_canvas)
        e2_var_date = tk.StringVar(header_canvas)
        e3_var_team = tk.StringVar(header_canvas)
        e4_var_month = tk.StringVar(header_canvas)
        e5_var_disc = tk.StringVar(header_canvas)
        e6_var_accomp_type = tk.StringVar(header_canvas)
        e7_var_team_mem = tk.StringVar(header_canvas)
        e8_var_project = tk.StringVar(header_canvas)
        e9_var_env = tk.StringVar(header_canvas)
        e10_var_defect = tk.StringVar(header_canvas)

        e1_var_id.set(s[0])
        e2_var_date.set(s[1])
        e3_var_team.set(s[2])
        e4_var_month.set(s[3])
        e5_var_disc.set(s[4])
        e6_var_accomp_type.set(s[5])
        e7_var_team_mem.set(s[6])
        e8_var_project.set(s[7])
        e9_var_env.set(s[8])
        e10_var_defect.set(s[9])

        e1 = tk.Entry(header_canvas, textvariable=e1_var_id, width=20, state='disabled').grid(row=i + 1, column=0)
        e2 = tk.Entry(header_canvas, textvariable=e2_var_date, width=20).grid(row=i + 1, column=1)
        e3 = tk.Entry(header_canvas, textvariable=e3_var_team, width=20).grid(row=i + 1, column=2)
        e4 = tk.Entry(header_canvas, textvariable=e4_var_month, width=20).grid(row=i + 1, column=3)
        e5 = tk.Entry(header_canvas, textvariable=e5_var_disc, width=20).grid(row=i + 1, column=4)
        e6 = tk.Entry(header_canvas, textvariable=e6_var_accomp_type, width=20).grid(row=i + 1, column=5)
        e7 = tk.Entry(header_canvas, textvariable=e7_var_team_mem, width=20).grid(row=i + 1, column=6)
        e8 = tk.Entry(header_canvas, textvariable=e8_var_project, width=20).grid(row=i + 1, column=7)
        e9 = tk.Entry(header_canvas, textvariable=e9_var_env, width=20).grid(row=i + 1, column=8)
        e10 = tk.Entry(header_canvas, textvariable=e10_var_defect, width=20).grid(row=i + 1, column=9)
        globals()[f'edit_button{i}'] = ttk.Button(header_canvas, text='update', command=lambda: my_update())
        globals()[f'edit_button{i}'].grid(row=i + 1, column=11, sticky='w')

        def my_update():
            print('hello')

            #             date2         = "".join(str(date) for date in e2_var_date.get())
            #             print(date2)
            #             print(list)
            #             print(type(str(date2)))
            date2 = e2_var_date.get()
            team2 = e3_var_team.get()
            month2 = e4_var_month.get()
            discription2 = e5_var_disc.get()
            type2 = e6_var_accomp_type
            team_member2 = e7_var_team_mem.get()
            project2 = e8_var_project.get()
            env2 = e9_var_env.get()
            defect2 = e10_var_defect.get()
            id2 = int(e1_var_id.get())

            #              print(type(id2))

            data2 = (date2, team2, month2, discription2, type2, team_member2, project2, env2, defect2)

            sql3 = '''UPDATE ACCOMP_SHEET SET DATE_PICK = %s, TEAM_NAME = %s, MONTH_NAME = %s, 
            DISCRIPTION = %s, ACCOMPLISHMENT_TYPE = %s, TEAM_MEM_NAMES = %s, PROJECT_NAME = %s, ENVIRONMENT = %s, 
            DEFECT_DETAIL = %s
            WHERE ID = 2 '''

            id = cursor.execute(sql3, data2)
            print(id.rowcount)

    #             for each in header_canvas.grid_slaves(i):
    #                 each.grid_forget()
    #                 diplay_table()

    #
    date = tk.StringVar()
    date_label = ttk.Label(main, text='Date')  # Date
    date_label.grid(row=0, column=0, sticky='w')
    cal = DateEntry(main, width=16, background="magenta3", foreground="white", bd=2, textvariable=date)
    cal.grid(row=0, column=1, sticky='w')
    #
    team_name = tk.StringVar()
    team_label = ttk.Label(main, text='Team Name')  # Team name
    team_label.grid(row=1, column=0, sticky='w')
    team_name_entry = ttk.Entry(main, width=40, textvariable=team_name)
    team_name_entry.grid(row=1, column=1, sticky='w')
    team_name_entry.focus()
    #
    months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
              'November', 'December']
    month_name = tk.StringVar()
    month_name.set('January')
    month_label = ttk.Label(main, text='Month')  # Month
    month_label.grid(row=2, column=0, sticky='w')
    month_drop_down = tk.OptionMenu(main, month_name, *months)
    month_drop_down.grid(row=2, column=1, sticky='w')
    #
    discription_name = tk.StringVar()
    discription_label = ttk.Label(main, text='Discription')  # Discription
    discription_label.grid(row=3, column=0, sticky='w')
    discription_entry = ttk.Entry(main, width=100, textvariable=discription_name)
    discription_entry.grid(row=3, column=1, sticky='w')
    discription_entry.focus()
    #
    type = ['Delivery Milestone', 'Accomplishment']
    type_name = tk.StringVar()
    type_name.set('Delivery Milestone')
    type_label = ttk.Label(main, text='Type')  # Delivery Milestone/Accomplishment
    type_label.grid(row=4, column=0, sticky='w')
    type_drop_down = tk.OptionMenu(main, type_name, *type)
    type_drop_down.grid(row=4, column=1, sticky='w')
    #
    team_mem_name = tk.StringVar()
    team_mem_label = ttk.Label(main, text='Team Members')  # Team Member
    team_mem_label.grid(row=5, column=0, sticky='w')
    team_mem_entry = ttk.Entry(main, width=100, textvariable=team_mem_name)
    team_mem_entry.grid(row=5, column=1, sticky='w')
    team_mem_entry.focus()
    #
    project_name = tk.StringVar()
    project_label = ttk.Label(main, text='Project')  # Project Name
    project_label.grid(row=6, column=0, sticky='w')
    project_entry = ttk.Entry(main, width=40, textvariable=project_name)
    project_entry.grid(row=6, column=1, sticky='w')
    project_entry.focus()
    #
    environment = ['DEV', 'PROD', 'UAT']
    environment_name = tk.StringVar()
    environment_name.set('DEV')
    environment_label = ttk.Label(main, text='Environment')  # environment
    environment_label.grid(row=7, column=0, sticky='w')
    environment_drop_down = tk.OptionMenu(main, environment_name, *environment)
    environment_drop_down.grid(row=7, column=1, sticky='w')
    #
    defect_name = tk.StringVar()
    defect_label = ttk.Label(main, text='Defect detail')  # Defect Details
    defect_label.grid(row=8, column=0, sticky='w')
    defect_entry = ttk.Entry(main, width=50, textvariable=defect_name)
    defect_entry.grid(row=8, column=1, sticky='w')

    # button
    submit_button = ttk.Button(main, text='SUBMIT', command=submit)
    submit_button.grid(row=15, column=0, sticky='s')
    submit_button = ttk.Button(main, text='REFRESH', command=quit_func)
    submit_button.grid(row=15, column=1, sticky='s')
    submit_button = ttk.Button(main, text='RESET FORM', command=reset)
    submit_button.grid(row=15, column=2, sticky='s')

    header_canvas = tk.Canvas(main, background="dark sea green", width=600, height=500, highlightthickness=0)
    header_canvas.grid(row=17, columnspan=2, sticky='w')
    table_button = ttk.Button(header_canvas, text='Display Table', command=diplay_table)
    table_button.grid(row=0, column=0, sticky='w')

    #     table_button = ttk.Button(header_canvas, text='Display Table', command=diplay_table)
    #     table_button.grid(row=0, column=0,sticky = 'w')

    root.mainloop()


if __name__ == '__main__':
    root = tk.Tk()
    root.title('Accomplishment Sheet')
    root.geometry("1500x500")
    root.configure(background='pink')
    design()