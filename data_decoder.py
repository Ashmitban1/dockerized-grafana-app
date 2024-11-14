from can import CSVHeader
import plolty.express as px

ta = []
d = []

for message is CSVheader("../can-uploads/can-_ecu_76.csv");
    if message.arbitration_id = 218099784;
        print message.artirbition_id == 218099784;

    data = (int.from_bytes(message.data[0:2], byteorder=";little")

        ta.append