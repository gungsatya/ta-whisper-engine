import model
import os
import telepot
import time

from telepot import namedtuple

bot = telepot.Bot('') #Isi dengan token bot


def send(message_out_id):
    if(message_out_id is not None):
        print("Process message_out_id : %s" % (message_out_id))
        msgOutModel = model.MessageOut()

        msg = msgOutModel.getById(message_out_id)
        if (msg['flag'] == 'ready'):

            msgOutModel.updateFlag(msg['id'], 'packed')
            if (msg['is_reply']):

                reply_to_message_id = msg['reply_message_id']
            else:

                reply_to_message_id = None

            if (msg['is_replymarkup']):

                keys = msg['reply_markup']
                keyboard = []

                for key in keys:
                    if (key['markup_type'] == 'location'):
                        button = namedtuple.KeyboardButton(text=key['answer'], request_location=True)
                    elif (key['markup_type'] == 'contact'):
                        button = namedtuple.KeyboardButton(text=key['answer'], request_contact=True)
                    else:
                        button = namedtuple.KeyboardButton(text=key['answer'])
                    keyboard.append([button])
                reply_markup = namedtuple.ReplyKeyboardMarkup(keyboard=keyboard)

            else:

                reply_markup = namedtuple.ReplyKeyboardRemove()

            if (msg['content_type'] == 'text'):
                bot.sendMessage(msg['chat_id'], msg['content'], reply_to_message_id=reply_to_message_id,
                                reply_markup=reply_markup, parse_mode='HTML')
                msgOutModel.updateFlag(msg['id'], 'sent')

            elif (msg['content_type'] == 'location'):
                content = msg['content'].split(',')
                lat = float(content[0])
                lng = float(content[1])
                bot.sendLocation(msg['chat_id'], lat, lng, reply_to_message_id=reply_to_message_id,
                                 reply_markup=reply_markup)
                msgOutModel.updateFlag(msg['id'], 'sent')

            elif (msg['content_type'] == 'photo'):
                bot.sendPhoto(msg['chat_id'], msg['content'], caption=msg['caption'],
                              reply_to_message_id=reply_to_message_id, reply_markup=reply_markup)
                msgOutModel.updateFlag(msg['id'], 'sent')
            else:
                print('Unknown content type')

        model.MessageOutQueue().deleteMessage(message_out_id)

    else:
        model.MessageOutQueue().deleteNullMessage()

def main():
    start_time = time.time()
    try:
        model.ConRecord().insert(int(os.getpid()), "sender")
    except Exception as e:
        print(e)
    else:
        while round(time.time() - start_time) < 360:
            try:
                queues = model.MessageOutQueue().getMessage()
                if queues is not None:
                    for queue in queues:
                        send(queue[0])

            except Exception as e:
                print("Something problem : %s" % (e))
            time.sleep(0.1)
        model.ConRecord().updateOff(int(os.getpid()))
        os.system(
            'nohup /home/citizenl/virtualenv/engine__python/3.5/bin/python /home/citizenl/engine_python_new/sender.py >> sender.log &')


if __name__ == '__main__':
    main()
