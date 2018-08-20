import json
import pymysql
import sqlite3
from datetime import datetime

from DBUtils.SteadyDB import connect

db = connect(
    creator=pymysql,  # the rest keyword arguments belong to pymysql
    host="", user='', password='', database='', #data autentifikasi dari database
    autocommit=True, charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor)

sqlite = '' #letak SQLite

class QueryRunner:

    def run(self, query):
        c = db.cursor()
        c.execute(query)
        c.close()

    def runGetResults(self, query):
        c = db.cursor()
        queries = str(query)
        for command in queries.split(";"):
            if command.strip():
                c.execute(command)
                db.commit()
        data = c.fetchall()
        c.close()
        return data

    def runGetResult(self, query):
        c = db.cursor()
        queries = str(query)
        for command in queries.split(";"):
            if command.strip():
                c.execute(command)
                db.commit()
        data = c.fetchone()
        c.close()
        return data

class MessageIn:

    def insert(self, chat_id, update_id, message_id, username, surename, content_type, content):
        c = db.cursor()
        value = pymysql.escape_string(str(content))
        c.execute(
            '''INSERT INTO `message_in`(chat_id, update_id, message_id, username, surename, content_type, content, received_at)'''
            '''VALUE(%i,%i,%i,'%s','%s','%s','%s', NOW(3))''' % (
                chat_id, update_id, message_id, username, surename, content_type, value))
        c.close()

    def getAll(self):
        c = db.cursor()
        c.execute('''SELECT * FROM `message_in`''')
        data = c.fetchall()
        c.close()
        return data

    def getLast(self):
        c = db.cursor()
        c.execute('''SELECT * FROM `message_in` ORDER BY id DESC LIMIT 1''')
        data = c.fetchone()
        c.close()
        return data

    def getById(self, id):
        c = db.cursor()
        c.execute('''SELECT * FROM `message_in` WHERE `id`=%i''' % (id))
        result = c.fetchone()
        c.close()
        return result

    def updateFlag(self, id, flag):
        c = db.cursor()
        c.execute('''UPDATE `message_in` SET `flag`='%s' WHERE id=%i''' % (flag, id))
        c.close()

class MessageOut:

    def insert(self, chat_id, content_type, content,
               is_reply=0, reply_message_id=None, is_replymarkup=0, reply_markup=None,
               caption=None):
        c = db.cursor()
        if (reply_message_id is None): reply_message_id = '''NULL'''
        content = pymysql.escape_string(str(content))
        reply_markup = json.dumps(reply_markup)
        caption = str(caption).replace("'", "")
        query = '''INSERT INTO `message_out`(chat_id, reply_message_id, is_reply, content_type, content, is_replymarkup, reply_markup, caption, created_at )VALUE(%i, %s, %i, '%s', '%s', %i, '%s','%s', NOW(3))''' % \
                (chat_id, reply_message_id, is_reply, content_type, content, is_replymarkup, reply_markup, caption)
        c.execute(query)
        MessageOutQueue().insert(c.lastrowid)
        c.close()

    def getById(self, id):
        c = db.cursor()
        c.execute('''SELECT * FROM `message_out` WHERE `id`=%i ''' % (id))
        data = c.fetchone()
        if data is not None:
            if data['is_replymarkup']:
                data['reply_markup'] = json.loads(data['reply_markup'])
        c.close()
        return data

    def getAll(self):
        c = db.cursor()
        c.execute('''SELECT * FROM `message_out` WHERE `flag` = 'ready' ORDER BY id ASC''')
        results = c.fetchall()
        data = []
        tmp = 0
        for result in results:
            data.append(result)
            data[tmp]['reply_markup'] = json.loads(result['reply_markup'])
            tmp += 1
        c.close()
        return data

    def updateFlag(self, id, flag):
        c = db.cursor()
        if (flag == 'sent'):
            c.execute('''UPDATE `message_out` SET `flag`='%s', `sent_at`=NOW(3) WHERE `id`=%i''' % (
            flag, id))
        else:
            c.execute('''UPDATE `message_out` SET `flag`='%s' WHERE `id`=%i''' % (flag, id))
        c.close()

class PatternControl:

    def insert(self, chat_id):
        c = db.cursor()
        c.execute('''INSERT INTO `pattern_control`(chat_id) VALUE(%i)''' % (chat_id))
        c.close()

    def getPathControlByChatId(self, chat_id):
        c = db.cursor()
        c.execute('''SELECT * FROM `pattern_control` WHERE `chat_id`=%i''' % (chat_id))
        data = c.fetchone()
        c.close()
        return data

    def changeOpsId(self, chat_id, operation_id):
        c = db.cursor()
        c.execute('''UPDATE pattern_control SET `current_ops_id`=%s WHERE `chat_id`=%i''' % (operation_id, chat_id))
        c.close()

    def changeParamInId(self, chat_id, param_in_id):
        c = db.cursor()
        c.execute(
            '''UPDATE pattern_control SET `current_param_in_id`=%s WHERE `chat_id`=%i''' % (param_in_id, chat_id))
        c.close()

    def changePatternId(self, chat_id, pattern_id):
        c = db.cursor()
        c.execute('''UPDATE pattern_control SET `current_pattern_id`=%s WHERE `chat_id`=%i''' % (pattern_id, chat_id))
        c.close()

    def changePatternParamId(self, chat_id, pattern_param_id):
        c = db.cursor()
        c.execute(
            '''UPDATE pattern_control SET `current_pattern_param_id`=%s WHERE `chat_id`=%s''' % (
            pattern_param_id, chat_id))
        c.close()

    def changeCurrentProcessed(self, chat_id, current_processed):
        c = db.cursor()
        c.execute(
            '''UPDATE pattern_control SET `current_processed`='%s' WHERE `chat_id`=%s''' % (current_processed, chat_id))
        c.close()

    def changeCurrentSurveyId(self, chat_id, current_survey_id):
        c = db.cursor()
        c.execute(
            '''UPDATE pattern_control SET `current_survey_id`=%s WHERE `chat_id`=%i''' % (current_survey_id, chat_id))
        c.close()

    def changeCurrentSurveyQuestionId(self, chat_id, current_survey_question_id):
        c = db.cursor()
        c.execute(
            '''UPDATE pattern_control SET `current_survey_question_id`=%s WHERE `chat_id`=%i''' % (current_survey_question_id, chat_id))
        c.close()

    def changeTempSurveyId(self, chat_id, temp_survey_id):
        c = db.cursor()
        c.execute(
            '''UPDATE pattern_control SET `temp_survey_id`=%s WHERE `chat_id`=%i''' % (temp_survey_id, chat_id))
        c.close()

class Operation:

    def getAll(self):
        c = db.cursor()
        c.execute('''SELECT * FROM `operation`''')
        data = c.fetchall()
        c.close()
        return data

    def getBySyntax(self, syntax):
        c = db.cursor()
        c.execute('''SELECT * FROM `operation` WHERE `in_syntax`='%s' ''' % (syntax))
        data = c.fetchone()
        c.close()
        return data

    def getById(self, id):
        c = db.cursor()
        c.execute('''SELECT * FROM `operation` WHERE `id`=%i''' % (id))
        data = c.fetchone()
        c.close()
        return data

class ErrorMessage:

    def sendToMsgOut(self, chat_id, message_id, error_message_id, er_is_replymarkup=0, er_reply_markup=None):
        c = db.cursor()

        c.execute('''SELECT * FROM error_message WHERE `id`=%i''' % (error_message_id))
        msg = c.fetchone()
        MessageOut().insert(chat_id, 'text', msg['content'],
                            is_reply=1, reply_message_id=message_id, is_replymarkup=er_is_replymarkup,
                            reply_markup=er_reply_markup)
        c.close()

class ParamIn:

    def getById(self, id):
        c = db.cursor()

        c.execute('''SELECT * FROM `param_in` WHERE `id`=%i''' % (id))
        data = c.fetchone()
        c.close()
        return data

    def getByOpsId(self, operation_id):
        c = db.cursor()

        c.execute('''SELECT * FROM `param_in` WHERE `operation_id`=%i ORDER BY `param_in_order` ASC''' % (operation_id))
        data = c.fetchall()
        c.close()
        return data

class Pattern:

    def insert(self, chat_id, operation_id):
        c = db.cursor()

        c.execute('''INSERT INTO `pattern`(chat_id, operation_id, created_at) VALUE(%i,%i,NOW(3))''' % (
        chat_id, operation_id))
        data = c.lastrowid
        return data

    def changeFlag(self, id, flag):
        c = db.cursor()
        c.execute('''UPDATE `pattern` SET `flag`='%s' WHERE `id`=%i''' % (flag, id))
        c.close()

    def getById(self, id):
        c = db.cursor()
        c.execute('''SELECT * FROM `pattern` WHERE `id`=%i''' % (id))
        data = c.fetchone()
        c.close()
        return data

class PatternParam:

    def insert(self, pattern_id, message_in_id, param, param_value):
        value = pymysql.escape_string(str(param_value))
        c = db.cursor()
        c.execute(
            '''INSERT INTO `pattern_param`(pattern_id,  message_in_id, param, param_value, created_at) VALUE(%i,%i,'%s','%s', NOW(3))'''
            % (pattern_id, message_in_id, param, value))
        data = c.lastrowid
        c.close()
        return data

    def getAllParamValue(self, pattern_id):
        c = db.cursor()
        c.execute('''SELECT * FROM `pattern_param` WHERE `pattern_id`=%i ORDER BY id DESC'''
                  % (pattern_id))
        data = c.fetchall()
        c.close()
        return data

    def getParamValue(self, pattern_id, param):
        c = db.cursor()
        c.execute('''SELECT * FROM `pattern_param` WHERE `pattern_id`=%i AND `param`='%s' ''' % (pattern_id, param))
        param = c.fetchone()
        c.close()
        return param['param_value']

    def countParam(self, pattern_id, param):
        c = db.cursor()
        c.execute('''SELECT count(id) AS `total` FROM `pattern_param` WHERE `pattern_id`=%i AND `param`='%s' '''
                  % (pattern_id, param))
        result = c.fetchone()
        c.close()
        return result['total']

    def getById(self, id):
        c = db.cursor()
        c.execute('''SELECT * FROM `pattern_param` WHERE `id`=%i''' % (id))
        data = c.fetchone()
        c.close()
        return data

    def addValueParam(self, id, value):
        c = db.cursor()
        param = self.getById(id)
        new_value = param['param_value'] + "\n" + value
        c.execute('''UPDATE pattern_param SET `param_value`='%s' WHERE id=%i''' %
                  (new_value, id))
        c.close()

    def deleteByParam(self, pattern_id, param):
        c = db.cursor()
        c.execute('''DELETE FROM pattern_param WHERE `pattern_id`=%i AND `param`='%s' '''
                  % (pattern_id, param))
        c.close()

class AnswerOption:

    def getByParamInId(self, param_in_id):
        c = db.cursor()
        c.execute('''SELECT * FROM `answer_text_option` WHERE `param_in_id`=%i''' % (param_in_id))
        data = c.fetchall()
        c.close()
        return data

class ParamOut:

    def getByOpsId(self, operation_id):
        c = db.cursor()
        c.execute('''SELECT * FROM `param_out` WHERE `operation_id`=%i''' % (operation_id))
        data = c.fetchall()
        c.close()
        return data

class Command:

    def insert(self, operation_id, chat_id, sql_query):
        c = db.cursor()
        c.execute(
            '''INSERT INTO `command`(operation_id, chat_id, sql_query, created_at) VALUE(%i,%i,"%s", NOW())''' % (
            operation_id, chat_id, sql_query))
        data = c.lastrowid
        c.close()
        return data

    def getOne(self):
        c = db.cursor()
        c.execute('''SELECT * FROM `command` WHERE `flag`="ready" ORDER BY `id` LIMIT 1''')
        data = c.fetchone()
        c.close()
        return data

    def changeFlag(self, id, flag):
        c = db.cursor()
        c.execute('''UPDATE `command` SET `flag`='%s' WHERE `id`=%i''' % (flag, id))
        c.close()

class ConRecord:

    def insert(self, PID, engine_name):
        db_sqlite = sqlite3.connect(sqlite)
        c = db_sqlite.cursor()
        c.execute(
            '''INSERT INTO engine_con_record(engine_name, PID, start_time)VALUES('%s', %i, '%s')''' % (
            engine_name, PID, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        db_sqlite.commit()
        c.close()
        db_sqlite.close()

    def updateOff(self, PID):
        db_sqlite = sqlite3.connect(sqlite)
        c = db_sqlite.cursor()
        c.execute('''UPDATE `engine_con_record` SET `is_active`=0 WHERE `PID`=%i''' % (PID))
        db_sqlite.commit()
        c.close()
        db_sqlite.close()

class MessageOutQueue:

    def insert(self, message_in_id):
        db_sqlite = sqlite3.connect(sqlite)
        c = db_sqlite.cursor()
        c.execute('''INSERT INTO `message_out_queue`(`message_out_id`)VALUES(%i)''' % (message_in_id))
        db_sqlite.commit()
        c.close()

    def getMessage(self):
        db_sqlite = sqlite3.connect(sqlite)
        c = db_sqlite.cursor()
        c.execute(
            '''SELECT * FROM `message_out_queue` ORDER BY message_out_id ASC LIMIT 5''')
        data = c.fetchall()
        c.close()
        db_sqlite.close()
        return data

    def deleteMessage(self, message_out_id):
        db_sqlite = sqlite3.connect(sqlite)
        c = db_sqlite.cursor()
        c.execute('''DELETE FROM `message_out_queue` WHERE `message_out_id`=%i''' % (message_out_id))
        db_sqlite.commit()
        c.close()
        db_sqlite.close()
    
    def deleteNullMessage(self):
        db_sqlite = sqlite3.connect(sqlite)
        c = db_sqlite.cursor()
        c.execute('''DELETE FROM `message_out_queue` WHERE `message_out_id` IS NULL''')
        db_sqlite.commit()
        c.close()
        db_sqlite.close()

class MessageInQueueOps:

    def getMessage(self):
        db_sqlite = sqlite3.connect(sqlite)
        c = db_sqlite.cursor()
        c.execute(
            '''SELECT * FROM `message_in_queue_ops` ORDER BY message_in_id ASC LIMIT 5''')
        data = c.fetchall()
        c.close()
        db_sqlite.close()
        return data

    def deleteMessage(self, message_out_id):
        db_sqlite = sqlite3.connect(sqlite)
        c = db_sqlite.cursor()
        c.execute('''DELETE FROM `message_in_queue_ops` WHERE `message_in_id`=%i''' % (message_out_id))
        db_sqlite.commit()
        c.close()
        db_sqlite.close()
    
    def deleteNullMessage(self):
        db_sqlite = sqlite3.connect(sqlite)
        c = db_sqlite.cursor()
        c.execute('''DELETE FROM `message_in_queue_ops` WHERE `message_in_id` IS NULL''')
        db_sqlite.commit()
        c.close()
        db_sqlite.close()

class MessageInQueueSurvey:

    def getMessage(self):
        db_sqlite = sqlite3.connect(sqlite)
        c = db_sqlite.cursor()
        c.execute(
            '''SELECT * FROM `message_in_queue_survey` ORDER BY message_in_id ASC LIMIT 5''')
        data = c.fetchall()
        c.close()
        db_sqlite.close()
        return data

    def deleteMessage(self, message_out_id):
        db_sqlite = sqlite3.connect(sqlite)
        c = db_sqlite.cursor()
        c.execute('''DELETE FROM `message_in_queue_survey` WHERE `message_in_id`=%i''' % (message_out_id))
        db_sqlite.commit()
        c.close()
        db_sqlite.close()
    
    def deleteNullMessage(self):
        db_sqlite = sqlite3.connect(sqlite)
        c = db_sqlite.cursor()
        c.execute('''DELETE FROM `message_in_queue_survey` WHERE `message_in_id` IS NULL''')
        db_sqlite.commit()
        c.close()
        db_sqlite.close()

class Survey:

    def getById(self, id):
        c = db.cursor()
        c.execute('''SELECT * FROM `survey` WHERE `id`=%i AND due_At > NOW() ''' % (id))
        data = c.fetchone()
        c.close()
        return data

    def getQuestionById(self, id):
        c = db.cursor()
        c.execute('''SELECT * FROM `survey_question` WHERE `id`=%i''' % (id))
        data = c.fetchone()
        c.close()
        return data

    def getQuestionBySurveyId(self, survey_id):
        c = db.cursor()
        c.execute('''SELECT * FROM `survey_question` WHERE `survey_id`=%i''' % (survey_id))
        data = c.fetchall()
        c.close()
        return data

    def insertRespond(self, survey_id, survey_question_id, chat_id, respond):
        c = db.cursor()
        c.execute('''INSERT INTO `survey_respond`(survey_id, survey_question_id, chat_id, respond) VALUE(%i, %i, %i, '%s')'''
                  % (survey_id, survey_question_id, chat_id, respond))
        c.close()

    def getRespond(self, survey_question_id, chat_id):
        c = db.cursor()
        c.execute('''SELECT * FROM `survey_respond` WHERE `survey_question_id`=%i AND `chat_id`=%i'''
                  %(survey_question_id, chat_id))
        data = c.fetchall()
        c.close()
        return data
        
    def countRespond(self, survey_question_id, chat_id):
        c = db.cursor()
        c.execute('''SELECT count(id) AS `total` FROM `survey_respond` WHERE `survey_question_id`=%i AND `chat_id`=%i'''
                  % (survey_question_id, chat_id))
        result = c.fetchone()
        c.close()
        return result['total']