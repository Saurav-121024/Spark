import os
import shutil
import time
from functools import partial
from tkinter import *
from tkinter import filedialog, messagebox
import openpyxl

import pandas as pd
from dask import dataframe as dd


def design():
    global win
    file_variant = ('None', 'Client', 'Header', 'Both')

    win.geometry("1250x600")

    win_frame = Frame(win)
    win_frame.pack(fill=BOTH, expand=1)

    win_canvas = Canvas(win_frame, bg="dark sea green")
    win_canvas.pack(side=LEFT, fill=BOTH, expand=1)

    win_scrollbar = Scrollbar(win_frame, orient=VERTICAL, command=win_canvas.yview)
    win_scrollbar.pack(side=RIGHT, fill=Y)
    win_canvas.configure(yscrollcommand=win_scrollbar.set)
    win_canvas.bind('<Configure>', lambda e: win_canvas.configure(scrollregion=win_canvas.bbox("all")))

    root = Frame(win_canvas)

    win_canvas.create_window((0, 0), window=root, anchor='nw')

    root['bg'] = 'dark sea green'
    header_canvas = Canvas(root, background="dark sea green", width=600, height=50, highlightthickness=0)
    header_canvas.grid(row=0, column=0, sticky='n')
    win.title("File Converter")
    root.filename = []
    root.destination = []
    root.delete_file = []

    def convert_file(num):
        global output_limit
        global final_file
        if globals()[f"header{num}"].cget('text') == '' and (
                globals()[f"hdr_choice{num}"].get() == '' or globals()[f"hdr_choice{num}"].get() == 'None'):

            if (int(globals()[f"file_size{num}"].cget('text')) < int(output_limit)) or int(output_limit) == 0:
                user_data_frame = pd.read_csv(root.filename[num], sep=deli_text.get(), header=None, chunksize=50000)
                file = f"{str(root.destination) + '/' + str((globals()[f'file{num}'].cget('text').split('.'))[0])}.csv"
                if os.path.exists(file) and os.path.isfile(file):
                    os.remove(file)
                for chunk in user_data_frame:
                    chunk.to_csv(
                        f"{str(root.destination) + '/' + str((globals()[f'file{num}'].cget('text').split('.'))[0])}.csv",
                        header=False, index=False, mode='a')
                final_file[
                    f"{str((globals()[f'file{num}'].cget('text').split('.'))[0])}.csv"] = \
                    globals()[f"count{num}"].cget('text')
            else:
                """ file distribution on the basis of size of file"""
                user_data_frame = dd.read_csv(root.filename[num], sep=deli_text.get(), header=0,
                                              blocksize=output_limit * 1024 * 1024)
                file_info_data = user_data_frame.map_partitions(len).compute()
                for sz in range(file_info_data.size):
                    final_file[
                        f"{str((globals()[f'file{num}'].cget('text').split('.'))[0])}.fl{sz}.csv"] = \
                        file_info_data[sz]

                o = user_data_frame.to_csv(
                    f"{str(root.destination) + '/' + str((globals()[f'file{num}'].cget('text').split('.'))[0])}.fl*.csv",
                    index=False)

        elif globals()[f"header{num}"].cget('text') == '' and (
                globals()[f"hdr_choice{num}"].get() == 'Client' or globals()[f"hdr_choice{num}"].get() == 'Header'):

            if globals()[f"hdr_choice{num}"].get() == 'Client':
                add_suf = 'CLH'
            else:
                add_suf = 'H'
            if (int(globals()[f"file_size{num}"].cget('text')) < int(output_limit)) or int(output_limit) == 0:
                user_data_frame = pd.read_csv(root.filename[num], sep=deli_text.get(), header=None, chunksize=50000)
                file = f"{str(root.destination) + '/' + str((globals()[f'file{num}'].cget('text').split('.'))[0] + '.' + add_suf)}.csv"
                if os.path.exists(file) and os.path.isfile(file):
                    os.remove(file)
                for chunk in user_data_frame:
                    chunk.to_csv(
                        f"{str(root.destination) + '/' + str((globals()[f'file{num}'].cget('text').split('.'))[0]) + '.' + add_suf}.csv",
                        header=False, index=False, mode='a')
                final_file[
                    f"{str((globals()[f'file{num}'].cget('text').split('.'))[0]) + '.' + add_suf}.csv"] = \
                    globals()[f"count{num}"].cget('text')
            else:
                """ file distribution on the basis of size of file"""
                user_data_frame = dd.read_csv(root.filename[num], sep=deli_text.get(), header=0,
                                              blocksize=output_limit * 1024 * 1024)
                file_info_data = user_data_frame.map_partitions(len).compute()
                for sz in range(file_info_data.size):
                    final_file[
                        f"{str((globals()[f'file{num}'].cget('text').split('.'))[0])}.fl{sz}{add_suf}.csv"] = \
                        file_info_data[sz]

                o = user_data_frame.to_csv(
                    f"{str(root.destination) + '/' + str((globals()[f'file{num}'].cget('text').split('.'))[0])}.fl*{add_suf}.csv",
                    index=False)

        elif globals()[f"header{num}"].cget('text') != '' and (
                globals()[f"hdr_choice{num}"].get() == 'Client' or globals()[f"hdr_choice{num}"].get() == 'Header'):

            if globals()[f"hdr_choice{num}"].get() == 'Client':
                add_suf = 'CLH'
                hdr_file = pd.read_excel(detail_canvas.header, header=0)
                hdr_file_list = hdr_file['Client'].tolist()
            else:
                add_suf = 'H'
                hdr_file = pd.read_excel(detail_canvas.header, header=0)
                hdr_file_list = hdr_file['Headers'].tolist()

            if (int(globals()[f"file_size{num}"].cget('text')) < int(output_limit)) or int(output_limit) == 0:
                user_data_frame = pd.read_csv(root.filename[num], sep=deli_text.get(), header=0, names=hdr_file_list,
                                              chunksize=50000)
                file = f"{str(root.destination) + '/' + str((globals()[f'file{num}'].cget('text').split('.'))[0] + '.' + add_suf)}.csv"
                if os.path.exists(file) and os.path.isfile(file):
                    os.remove(file)
                for chunk in user_data_frame:
                    chunk.to_csv(
                        f"{str(root.destination) + '/' + str((globals()[f'file{num}'].cget('text').split('.'))[0]) + '.' + add_suf}.csv",
                        index=False, mode='a')
                final_file[
                    f"{str((globals()[f'file{num}'].cget('text').split('.'))[0]) + '.' + add_suf}.csv"] = \
                    globals()[f"count{num}"].cget('text')
            else:
                """ file distribution on the basis of size of file"""
                user_data_frame = dd.read_csv(root.filename[num], sep=deli_text.get(), header=0, names=hdr_file_list,
                                              blocksize=output_limit * 1024 * 1024)
                file_info_data = user_data_frame.map_partitions(len).compute()
                for sz in range(file_info_data.size):
                    final_file[
                        f"{str((globals()[f'file{num}'].cget('text').split('.'))[0])}.fl{sz}{add_suf}.csv"] = \
                        file_info_data[sz]

                o = user_data_frame.to_csv(
                    f"{str(root.destination) + '/' + str((globals()[f'file{num}'].cget('text').split('.'))[0])}.fl*{add_suf}.csv",
                    index=False)

        elif globals()[f"header{num}"].cget('text') != '' and globals()[f"hdr_choice{num}"].get() == 'Both':
            print("_______________________________elif2_____________________")
            add_suf1 = 'CLH'
            hdr_file1 = pd.read_excel(detail_canvas.header, header=0)
            hdr_file_list_cl = hdr_file1['Client'].tolist()

            add_suf = 'H'
            hdr_file = pd.read_excel(detail_canvas.header, header=0)
            hdr_file_list = hdr_file['Headers'].tolist()

            def Diff(li1, li2):
                return list(set(li1) - set(li2))

            # Driver Code

            del_column = Diff(hdr_file_list, hdr_file_list_cl)

            if (int(globals()[f"file_size{num}"].cget('text')) < int(output_limit)) or int(output_limit) == 0:
                user_data_frame = pd.read_csv(root.filename[num], sep=deli_text.get(), header=0, names=hdr_file_list,
                                              chunksize=50000)
                file = f"{str(root.destination) + '/' + str((globals()[f'file{num}'].cget('text').split('.'))[0] + '.' + add_suf)}.csv"
                if os.path.exists(file) and os.path.isfile(file):
                    os.remove(file)
                for chunk in user_data_frame:
                    chunk.to_csv(
                        f"{str(root.destination) + '/' + str((globals()[f'file{num}'].cget('text').split('.'))[0]) + '.' + add_suf}.csv",
                        index=False, mode='a')
                final_file[
                    f"{str((globals()[f'file{num}'].cget('text').split('.'))[0]) + '.' + add_suf}.csv"] = \
                    globals()[f"count{num}"].cget('text')
                """client processing"""
                user_data_frame1 = pd.read_csv(root.filename[num], sep=deli_text.get(), header=0, names=hdr_file_list,
                                               chunksize=50000)
                # user_data_frame.drop(columns=del_column, inplace=True)
                file = f"{str(root.destination) + '/' + str((globals()[f'file{num}'].cget('text').split('.'))[0] + '.' + add_suf1)}.csv"
                if os.path.exists(file) and os.path.isfile(file):
                    os.remove(file)
                for chunk in user_data_frame1:
                    chunk.drop(columns=del_column, inplace=True)
                    chunk.to_csv(
                        f"{str(root.destination) + '/' + str((globals()[f'file{num}'].cget('text').split('.'))[0]) + '.' + add_suf1}.csv",
                        index=False, mode='a')
                final_file[
                    f"{str((globals()[f'file{num}'].cget('text').split('.'))[0]) + '.' + add_suf1}.csv"] = \
                    globals()[f"count{num}"].cget('text')
            else:
                """ file distribution on the basis of size of file"""
                user_data_frame = dd.read_csv(root.filename[num], sep=deli_text.get(), header=0, names=hdr_file_list,
                                              blocksize=output_limit * 1024 * 1024)
                file_info_data = user_data_frame.map_partitions(len).compute()
                for sz in range(file_info_data.size):
                    final_file[
                        f"{str((globals()[f'file{num}'].cget('text').split('.'))[0])}.fl{sz}{add_suf}.csv"] = \
                        file_info_data[sz]

                o = user_data_frame.to_csv(
                    f"{str(root.destination) + '/' + str((globals()[f'file{num}'].cget('text').split('.'))[0])}.fl*{add_suf}.csv",
                    index=False)
                """Client processing with split"""
                user_data_frame1 = dd.read_csv(root.filename[num], sep=deli_text.get(), header=0, names=hdr_file_list,
                                               blocksize=output_limit * 1024 * 1024)
                user_data_frame2 = user_data_frame.drop(columns=del_column)

                file_info_data1 = user_data_frame2.map_partitions(len).compute()
                for sz in range(file_info_data1.size):
                    final_file[
                        f"{str((globals()[f'file{num}'].cget('text').split('.'))[0])}.fl{sz}{add_suf1}.csv"] = \
                        file_info_data[sz]

                o = user_data_frame2.to_csv(
                    f"{str(root.destination) + '/' + str((globals()[f'file{num}'].cget('text').split('.'))[0])}.fl*{add_suf1}.csv",
                    index=False)

        # else:
        #     print("_______________________________else_____________________")
        #     print(globals()[f"hdr_choice{num}"].get())
        #     print(f"filename---{root.filename[num]}")
        #     print(f"header{globals()[f'header{num}'].cget('text')}")

    def validate_header(t):
        if globals()[f'header{t}'].cget('text') == '' and globals()[f'hdr_choice{t}'].get() in file_variant[3:]:
            header_error = messagebox.showerror("Header error", f"For file {root.filename[t]}"
                                                                f" /n the combination of header file or end result"
                                                                f"file is not opted correctly.")
            return header_error
        elif globals()[f'header{t}'].cget('text') != '' and (
                globals()[f'hdr_choice{t}'].get() in file_variant[:1] or globals()[f'hdr_choice{t}'].get() == ''):
            header_error = messagebox.showerror("Header error", f"For file {root.filename[t]}"
                                                                f" /n the combination of header file or end result"
                                                                f"file is not opted correctly.")
            # elif globals()[f'header{t}'].cget('text') != '' and os.path.getsize(globals()[f'header{t}'].cget('text')) == 0:
            #     header_error = messagebox.showerror("Header File Empty", f"For file {root.filename[t]}"
            #                                                         f" /n the supplied header file is empty.")
            return header_error

    def conversion():
        """conversion"""
        global final_file
        val_result = False
        start = time.time()
        for t in range(len(root.filename)):
            if not val_result:
                val_result = validate_header(t)

        if not val_result:
            del_valid = False
            des_valid = False
            if deli_text.get() == '':
                messagebox.showerror("Delimiter error", "You have not provided any delimiter")
                del_valid = True

            print(root.destination)

            if not root.destination:
                messagebox.showerror("Destination error", "please provide destination path")
                des_valid = True

            if not del_valid and not des_valid:
                if row_per_text.get() == '':
                    globals()[f'output_limit'] = 0
                else:
                    globals()[f'output_limit'] = int(row_per_text.get())
                for j in range(len(root.filename)):
                    convert_file(j)
                r = 0
                for k, v in final_file.items():
                    r += 1
                    globals()[f"report{r}"] = Label(scrollable1_frame, text=k,
                                                    font=("Times", 15, 'italic'), width=50, bg='white', fg='black')
                    globals()[f"report{r}"].grid(row=r, column=0, sticky=W, pady=(0, 5), padx=(0, 10))

                    globals()[f"f_count{r}"] = Label(scrollable1_frame, text=f"{v}",
                                                     font=("Times", 15, 'italic'), width=30, bg='white', fg='black')
                    globals()[f"f_count{r}"].grid(row=r, column=1, sticky=W, pady=(0, 5), padx=(40, 0))
        finish = time.time()
        print(f"Total computation time is:{finish - start} second(s)")
        messagebox.showinfo(title="Conversion", message="Conversion process completed")

    def delete_file():
        should_delete = messagebox.askokcancel(title="Delete input Files", message="Are you sure you want to delete Files")
        if should_delete:
            for files in root.filename:
                os.remove(files)
            messagebox.showinfo(title="Delete status", message="Delete Process completed")
        pass

    def reset():
        """Reset function"""
        global win
        global final_file
        final_file.clear()
        win.destroy()

        win = Tk()

        design()

    def header_br(i):
        detail_canvas.header = filedialog.askopenfilename(initialdir=os.getcwd(), title="select files",
                                                          filetypes=(("Excel Files", ".xlsx"), ("all files", "*.*")))
        # ("text file", "*.txt "), ("all files", "*.*")))

        globals()[f"header{i}"].config(text=os.path.basename(detail_canvas.header))

    def source_br():
        """browse source"""

        if len(root.filename) == 0:
            root.filename = list(filedialog.askopenfilenames(initialdir=os.getcwd(), title="select files",
                                                             filetypes=(("text file", "*.txt "), ("all files", "*.*"))))
        else:
            reset_parm = messagebox.askokcancel(title=f"process file",
                                                message=f"Do you wish to add more files to the current selected files")

            if not reset_parm:
                pass
            else:
                root.filename += list(filedialog.askopenfilenames(initialdir=os.getcwd(), title="select files",
                                                                  filetypes=(
                                                                      ("text file", "*.txt "), ("all files", "*.*"))))

        if len(root.filename) > 1:
            source_add.config(text=os.path.commonpath(root.filename))
        else:
            source_add.config(text=os.path.dirname(os.path.commonpath(root.filename)))

        for i in range(len(root.filename)):
            globals()[f"file{i}"] = Label(scrollable_frame, width=50, font=("Times", 15, "italic"))
            globals()[f"file{i}"].config(text=os.path.basename(root.filename[i]), bg="white", fg='black')
            globals()[f"file{i}"].grid(row=i, column=0, padx=2, sticky='w')
            """header labels"""
            globals()[f"header{i}"] = Label(scrollable_frame, width=10, font=("Times", 15, "italic"))
            globals()[f"header{i}"].config(text="", bg="gray", fg='black')
            globals()[f"header{i}"].grid(row=i, column=1, padx=(15, 2), sticky='w')
            """header button"""
            globals()[f"hd_brw{i}"] = Button(scrollable_frame, text="B", bg="yellow", fg="black",
                                             font=("Times", 15, "italic"),
                                             command=partial(header_br, i))
            globals()[f"hd_brw{i}"].grid(row=i, column=2, padx=2, sticky='W')
            """count labels"""

            def buf_count_newlines_gen(fname):
                def _make_gen(reader):
                    b = reader(2 ** 16)
                    while b:
                        yield b
                        b = reader(2 ** 16)

                with open(fname, "rb") as f:
                    count = sum(buf.count(b"\n") for buf in _make_gen(f.raw.read))
                if os.path.getsize(fname) == 0:
                    return count
                else:
                    return count + 1

            count_line = buf_count_newlines_gen(root.filename[i])
            globals()[f"count{i}"] = Label(scrollable_frame, width=10, font=("Times", 15, "italic"))
            globals()[f"count{i}"].config(text=count_line, bg="light gray", fg='black')
            globals()[f"count{i}"].grid(row=i, column=3, padx=2, sticky='w')

            """Adding optional menu for type of headers"""
            globals()[f"hdr_choice{i}"] = StringVar(detail_canvas)
            option_choice = OptionMenu(scrollable_frame, globals()[f"hdr_choice{i}"], *file_variant)

            option_choice.grid(row=i, column=4, padx=(15, 2), sticky='w')

            """File size in MB"""
            globals()[f"file_size{i}"] = Label(scrollable_frame, width=5, font=("Times", 15, "italic"))
            globals()[f"file_size{i}"].config(text=f"{os.path.getsize(root.filename[i]) // (1024 * 1024)}",
                                              bg="light gray", fg='black')
            globals()[f"file_size{i}"].grid(row=i, column=5, padx=2, sticky='w')

    def destination_br():
        """browse source"""
        root.destination = filedialog.askdirectory(initialdir=os.getcwd(), title="choose the directory")
        destination_add.config(text=root.destination)

    def move_br():
        """moving source files to destination"""
        for i in range(0, len(root.filename)):

            try:
                b_name = os.path.basename(root.filename[i])
                shutil.move(root.filename[i], root.destination)
                # new_address = f"{os.path.join(root.destination, '', b_name)}"
                # root.filename[i] = new_address
                root.filename[i] = os.path.join(root.destination, b_name)
                root.filename[i] = root.filename[i].replace("\\", "/")

            except OSError:
                b_name = ""
                answer = messagebox.askokcancel(
                    message=f"{os.path.basename(root.filename[i])} already exist in {os.path.basename(root.destination)}\n"
                            f"Do you wish to replace the existing files/folder")
                if answer:
                    os.remove(os.path.join(root.destination, os.path.basename(root.filename[i])))
                    shutil.move(root.filename[i], root.destination)
            # finally:
            # b_name = os.path.basename(root.filename[i])
            # new_address = f"{os.path.join(root.destination, b_name)}"
            # root.filename[i] = new_address

        if len(root.filename) > 1:
            source_add.config(text=os.path.commonpath(root.filename))
        else:
            source_add.config(text=os.path.dirname(os.path.commonpath(root.filename)))

        destination_add.config(text="")

    """Panel Heading"""
    var = StringVar()
    label = Label(header_canvas, textvariable=var, relief=RAISED)
    var.set("Automated Text to Multi-File Conversion")
    label.config(bg='indian red', fg="black", font=("Helvetica", 20, "bold italic"), justify='right', bd=0)
    label.grid(row=0, column=1, columnspan=4, pady=20)

    """Creating the first canvas"""
    path_canvas = Canvas(root, width=600, height=100, background="dark sea green", highlightthickness=0)
    path_canvas.grid(row=1, column=0, pady=10, sticky='w')
    """Panel First Row"""

    source_path = Label(path_canvas, text="Source", font=("Times", 15, "italic"), width=10)
    source_path.config(bg="light goldenrod", fg='black')
    source_path.grid(row=1, column=1, padx=1, pady=2)

    source_add = Label(path_canvas, width=70, font=("Times", 15, "italic"))

    source_add.grid(row=1, column=2, pady=2, padx=5)

    source_br = Button(path_canvas, text="Browse", bg="gray", fg="black", font=("Times", 15, "italic"),
                       command=source_br)

    source_br.grid(row=1, column=3, pady=2)

    """Panel second row"""

    destination_path = Label(path_canvas, text="Destination", font=("Times", 15, "italic"), width=10)
    destination_path.config(bg="light goldenrod", fg='black')
    destination_path.grid(row=2, column=1, padx=1, pady=2)

    destination_add = Label(path_canvas, width=70, font=("Times", 15, "italic"))

    destination_add.grid(row=2, column=2, pady=2, padx=5)

    destination_br = Button(path_canvas, text="Browse", bg="gray", fg="black", command=destination_br,
                            font=("Times", 15, "italic"))

    destination_br.grid(row=2, column=3, padx=1, pady=2)

    move_br = Button(path_canvas, text="Move", bg="gray", fg="black", command=move_br, font=("Times", 15, "italic"))

    move_br.grid(row=2, column=4, pady=2, padx=5)

    """Canvas 3 containing delimiter and excel limit"""
    del_canvas = Canvas(root, width=50, height=400, background="dark sea green", highlightthickness=0)
    del_canvas.grid(row=2, columnspan=27, sticky='w')

    deli_meter = Label(del_canvas, text="Delimiter", font=("Times", 15, "italic"), width=20)
    deli_meter.config(bg="light goldenrod", fg='black')
    deli_meter.grid(row=2, column=0)

    deli_text = Entry(del_canvas, width=10)
    deli_text.grid(row=2, column=1, padx=5)

    """Maintaining the look and feel"""
    look_feel = Label(del_canvas, text="", font=("Times", 15, "italic"), width=10)
    look_feel.config(bg="dark sea green", highlightthickness=0)
    look_feel.grid(row=2, column=2)

    row_per_sheet = Label(del_canvas, text="Size Per File(MB)", font=("Times", 15, "italic"), width=20)
    row_per_sheet.config(bg="light goldenrod", fg='black')
    row_per_sheet.grid(row=2, column=3, pady=10)

    def checkNumberOnly(action, value_if_allowed):
        if action != '1':
            return True
        try:
            return value_if_allowed.isnumeric()
        except ValueError:
            return False

    v_cmd = (del_canvas.register(checkNumberOnly), '%d', '%P')
    row_per_text = Entry(del_canvas, width=10, validate='key', validatecommand=v_cmd)
    row_per_text.grid(row=2, column=4, padx=5, pady=10)

    """Maintaining the look and feel"""

    file_canvas = Canvas(root, width=1050, height=100, background="dark sea green",
                         highlightthickness=0)
    file_canvas.grid(row=3, column=0, sticky='w')

    file_name = Label(file_canvas, text="Filenames", font=("Times", 15, "italic"), width=50)
    file_name.config(bg="coral", fg='black')
    file_name.grid(row=0, column=0, sticky='w', ipadx=0, padx=0)

    header_name = Label(file_canvas, text="Header files", font=("Times", 15, "italic"), width=10)
    header_name.config(bg="coral", fg='black')
    header_name.grid(row=0, column=1, sticky='w', ipadx=0, padx=0)

    initial_count = Label(file_canvas, text="Initial count", font=("Times", 15, "italic"), width=10)
    initial_count.config(bg="coral", fg='black')
    initial_count.grid(row=0, column=2, padx=(20, 0), sticky='w')

    file_var = Label(file_canvas, text="File Type", font=("Times", 15, "italic"), width=7, anchor='w')
    file_var.config(bg="coral", fg='black')
    file_var.grid(row=0, column=3, ipadx=0, sticky='w')

    file_sz = Label(file_canvas, text="MB", font=("Times", 15, "italic"), width=4, anchor='w')
    file_sz.config(bg="coral", fg='black')
    file_sz.grid(row=0, column=4, ipadx=0, sticky='w')

    """creating a frame"""
    container = Frame(file_canvas, bg="dark sea green", bd=0, highlightthickness=0)
    container2 = Frame(file_canvas, bg="dark sea green", bd=0, highlightthickness=0)
    button_canvas = Canvas(container2, bg="dark sea green", bd=0, width=100, height=100, highlightthickness=0)
    button_canvas.grid(row=0, column=0, sticky=(W, N, E, S), padx=(20, 0))
    detail_canvas = Canvas(container, bg="dark sea green", highlightthickness=0, width=1000)
    clear_all = Button(button_canvas, text="Reset", command=reset, font=("Times", 15, "bold"), relief=SUNKEN, bg='gray',
                       fg='black')
    clear_all.grid(row=0, column=0)
    convert_all = Button(button_canvas, text="CONVERT", command=conversion, font=("Times", 15, "bold"), relief=SUNKEN,
                         bg='gray', fg='black')
    convert_all.grid(row=1, column=0, pady=20)

    delete_all = Button(button_canvas, text="Delete Text Files", command=delete_file, font=("Times", 15, "bold"),
                        relief=SUNKEN,
                        bg='gray', fg='black')
    delete_all.grid(row=2, column=0, pady=10)
    scrollbar = Scrollbar(container, orient="vertical", command=detail_canvas.yview)
    scrollable_frame = Frame(detail_canvas)
    """adjusting scroll region as per the frame content"""
    scrollable_frame.bind("<Configure>", lambda e: detail_canvas.configure(scrollregion=detail_canvas.bbox("all")))
    detail_canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
    scrollable_frame['bg'] = 'dark sea green'
    detail_canvas.configure(yscrollcommand=scrollbar.set)
    container.grid(row=1, column=0, columnspan=7, sticky="WE", pady=5)
    container2.grid(row=1, column=9, sticky="WE", pady=5)
    detail_canvas.grid(row=1, rowspan=5, column=0, sticky='WE')
    scrollbar.grid(row=1, rowspan=6, column=8, sticky="ns")

    """Report panel"""

    file_canvas2 = Canvas(root, width=1050, height=100, background="dark sea green",
                          highlightthickness=0)
    file_canvas2.grid(row=4, column=0, sticky='w')

    file_name1 = Label(file_canvas2, text="Filenames", font=("Times", 15, "italic"), width=50)
    file_name1.config(bg="coral", fg='black')
    file_name1.grid(row=0, column=0, sticky='w', ipadx=0, padx=0, pady=(5, 0))

    count_final = Label(file_canvas2, text="Final Count", font=("Times", 15, "italic"), width=30)
    count_final.config(bg="coral", fg='black')
    count_final.grid(row=0, column=1, columnspan=2, sticky='w', ipadx=0, padx=(20, 0), pady=(5, 0))

    """creating a frame"""
    container3 = Frame(file_canvas2, bg="dark sea green", bd=0, highlightthickness=0)
    detail1_canvas = Canvas(container3, bg="dark sea green", highlightthickness=0, width=1000)

    scrollbar1 = Scrollbar(container3, orient="vertical", command=detail1_canvas.yview)
    scrollable1_frame = Frame(detail1_canvas)
    """adjusting scroll region as per the frame content"""
    scrollable1_frame.bind("<Configure>", lambda e: detail1_canvas.configure(scrollregion=detail1_canvas.bbox("all")))
    detail1_canvas.create_window((0, 0), window=scrollable1_frame, anchor="nw")
    scrollable1_frame['bg'] = 'dark sea green'
    detail1_canvas.configure(yscrollcommand=scrollbar1.set)
    container3.grid(row=1, column=0, columnspan=7, sticky="WE", pady=5)
    detail1_canvas.grid(row=1, rowspan=5, column=0, sticky='WE')
    scrollbar1.grid(row=1, rowspan=6, column=8, sticky="ns")

    win.mainloop()


if __name__ == '__main__':
    count = 0
    win = Tk()
    final_file = {}
    design()
