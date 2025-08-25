import pandas as pd

df = pd.read_csv("output_data/rig_data_10000.csv")

message = {'Timestamp': '2025-01-01 02:46:35', 'Rig_ID': 'RIG_01', 'Depth': 20.049031394602487, 'WOB': 1423.38156392436, 'RPM': 78.48713437623728, 
                  'Torque': 431.7961678328709, 'ROP': 13.122869581376763, 'Mud_Flow_Rate': 1080.1095066075536, 'Mud_Pressure': 2859.746960496074, 
                  'Mud_Temperature': 64.93723448091345, 'Mud_Density': 1184.7164068520945, 'Mud_Viscosity': 38.572940706093966, 'Mud_PH': 8.386557664203348, 
                  'Gamma_Ray': 100.50012087340032, 'Resistivity': 16.902489806913415, 'Pump_Status': 1, 'Compressor_Status': 1, 'Power_Consumption': 194.80277238155932, 
                  'Vibration_Level': 0.4944634824340062, 'Bit_Temperature': 90.1118772693112, 'Motor_Temperature': 74.588674808601, 'Maintenance_Flag': 1,
                  'Failure_Type': "None"}

flag = message["Maintenance_Flag"]
ftype = [1 if not pd.isnull(message["Failure_Type"]) else 0][0]
print(ftype, flag)

if flag != ftype:
    message["Failure_Type"] = "Unknown_Error"

print(message["Maintenance_Flag"], message["Failure_Type"])