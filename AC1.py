import matplotlib.pyplot as plt
import numpy as np
import os
import h5py
import pandas as pd

import wfdb
import pdb

DB_ROOT = '/Users/nilo/Downloads/AC'
DB_ROOT_RAW = DB_ROOT + '/raw/'
DB_ROOT_PROC = DB_ROOT + '/processed/'

filename='AC_ds'
hdf5_filename=DB_ROOT_PROC+filename+'.h5'
csv_filename=DB_ROOT_PROC+filename+'.csv'

#record = wfdb.rdrecord('/Users/nilo/Downloads/AC/raw/04015',sampfrom=0, sampto=None)
#annot = wfdb.rdann('/Users/nilo/Downloads/AC/raw/04015','atr',sampfrom=0, sampto=15)

#fig = wfdb.plot_wfdb(record=record,return_fig=True)
#fig.savefig('plot.png')

# TODO: split record into data, label, header
f_reclist = open(DB_ROOT+'/RECORDS','r')
reclist = f_reclist.readlines()

head_label_csv = []

with h5py.File(hdf5_filename, 'w') as f:
    f.clear()
    data_grp = f.create_group('data')
    extra_grp = f.create_group('extra')

    # meta
    cnt=0
    #head_fs=[]
    #head_adc_gain=[]
    #head_adc_res=[]
    #head_adc_zero=[]

    for rcname_raw in reclist:
        # main data body
        rcname = rcname_raw.strip()
        record = wfdb.rdrecord(DB_ROOT+'/'+rcname,sampfrom=0, sampto=None)
        #annot = wfdb.rdann(DB_ROOT+'/'+rcname,'atr')

        sgnl_analog = record.p_signal
        sgnl_digital = record.adc()
        sgnl_len = record.sig_len
        annot_times = annot.sample
        annot_label = annot.symbol

        # items included in the header are not complete and should be extended on demand
        assert(record.fs==annot.fs)
        if cnt==0 :
            head_fs = record.fs
            head_adc_gain = record.adc_gain
            head_adc_res = record.adc_res
            head_adc_zero = record.adc_zero
            head_units = record.units
            annot.get_contained_labels()
            head_label_csv = annot.contained_labels
            cnt=1
        else:
            assert(head_fs == record.fs)
            assert(head_adc_gain == record.adc_gain)
            assert(head_adc_res == record.adc_res)
            assert(head_adc_zero == record.adc_zero)
            assert(head_units == record.units)
            annot.get_contained_labels()
            csv_new_entries = annot.contained_labels[~annot.contained_labels['label_store'].isin(head_label_csv['label_store'].index)]
            head_label_csv = head_label_csv.append(csv_new_entries)
    
        # insert into h5
        subgrp=data_grp.create_group(rcname)
        sa_dset = subgrp.create_dataset('sgnl_a', data=sgnl_analog, dtype=np.float32)
        sd_dset = subgrp.create_dataset('sgnl_d', data=sgnl_digital, dtype=np.uint32)
        sl_dset = subgrp.create_dataset('sgnl_length', data=sgnl_len, dtype=np.uint32)
        tm_dset = subgrp.create_dataset('times', data=annot_times, dtype=np.uint32)
        lbl_dset = subgrp.create_dataset('labels', data=annot_label, dtype=h5py.string_dtype('utf-8'))
    
    extra_grp.create_dataset('fs',data=head_fs)
    extra_grp.create_dataset('adc_gain',data=head_adc_gain)
    extra_grp.create_dataset('adc_res',data=head_adc_res)
    extra_grp.create_dataset('adc_zero',data=head_adc_zero)
    extra_grp.create_dataset('units',data=head_units)
    
head_label_csv = head_label_csv.sort_values(head_label_csv.columns[0])
head_label_csv.to_csv(csv_filename)

# debug
f=h5py.File(hdf5_filename,'r')
print(f['data']['100']['labels'][:])
print(f['extra']['fs'][()])
pdb.set_trace() 

# AC
