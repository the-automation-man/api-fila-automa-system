import redis
import dotenv
import os
import json
from flask import Flask, request, jsonify


dotenv.load_dotenv()

app = Flask(__name__)

# REDIS_URL = os.getenv('REDIS_URL')
redis_client = redis.Redis(
                    host=os.getenv('HOST_REDIS'),
                    port=os.getenv('REDIS_PORT'),
                    decode_responses=True,
                    username="default",
                    password=os.getenv('PASSWORD_REDIS'),
                    db=0
                    )

cod_queue = 'cod_queue'
edit_user_queue = 'edit_user_queue'

cod_processing_key = 'cod_processing_key'

edit_user_processing_key = 'edit_user_processing_key'


@app.route('/queue/add_user_cod', methods=['GET'])
def add_user_cod():

    user_id = request.args.get('id')


    if not user_id:
        return jsonify({'error': 'ID do usuário é obrigatório'})

    redis_client.rpush(cod_queue, json.dumps({'id': user_id}))

    return jsonify({'message': f'Usuário {user_id} adicionado à fila'})


@app.route('/queue/list_len_cod', methods=['GET'])
def list_len_cod():

    return jsonify({'tamanho_fila': redis_client.llen(cod_queue)})


@app.route('/queue/position/cod/', methods=['GET'])
def get_user_position_cod():

    user_id = request.args.get('id')

    queue = redis_client.lrange(cod_queue, 0, -1)

    for index, item in enumerate(queue):
        item_data = json.loads(item)
        if item_data.get('id') == user_id:
            return jsonify({'position': index + 1})

    return jsonify({'message': 'Usuário não encontrado na fila'}), 404


@app.route('/queue/next/cod', methods=['GET'])
def process_next_cod():

    if redis_client.hlen(cod_processing_key) > 0:
        return jsonify({'processing': list(redis_client.hgetall(cod_processing_key).keys())[0]})

    request_data = redis_client.lpop(cod_queue)

    if request_data:
        request_info = json.loads(request_data)
        user_id = request_info['id']

        redis_client.hset(cod_processing_key, user_id, json.dumps(user_id))

        return jsonify({'processing': user_id})

    return jsonify({'processing': 'Fila vazia'})


@app.route('/queue/completed/cod', methods=['GET'])
def mark_as_completed_cod():

    user_id = request.args.get('id')
    if redis_client.hdel(cod_processing_key, user_id):
        return jsonify({'message': f'Usuário {user_id} removido do processamento'})
    
    return jsonify({'error': 'Usuário não encontrado no processamento'}), 404






app.route('/queue/add_user_edit_user', methods=['GET'])
def add_user_edit_user():

    user_id = request.args.get('id')


    if not user_id:
        return jsonify({'error': 'ID do usuário é obrigatório'}), 400

    redis_client.rpush(edit_user_queue, json.dumps({'id': user_id}))

    return jsonify({'message': f'Usuário {user_id} adicionado à fila'})


@app.route('/queue/list_len_edit_user', methods=['GET'])
def list_len_edit_user():

    return jsonify({'tamanho_fila': redis_client.llen(edit_user_queue)})


@app.route('/queue/position/edit_user/', methods=['GET'])
def get_user_position_edit_user():

    user_id = request.args.get('id')

    queue = redis_client.lrange(edit_user_queue, 0, -1)

    for index, item in enumerate(queue):
        item_data = json.loads(item)
        if item_data.get('id') == user_id:
            return jsonify({'position': index + 1})

    return jsonify({'message': 'Usuário não encontrado na fila'}), 404


@app.route('/queue/next/edit_user', methods=['GET'])
def process_next_edit_user():

    if redis_client.hlen(edit_user_processing_key) > 0:
        return jsonify({'processing': list(redis_client.hgetall(cod_processing_key).keys())[0]})

    request_data = redis_client.lpop(edit_user_queue)

    if request_data:
        request_info = json.loads(request_data)
        user_id = request_info['id']

        redis_client.hset(edit_user_processing_key, user_id, json.dumps(request_info))

        return jsonify({'processing': user_id})

    return jsonify({'processing': 'Fila vazia'})


@app.route('/queue/completed/edit_user', methods=['GET'])
def mark_as_completed_edit_user():

    user_id = request.args.get('id')
    if redis_client.hdel(edit_user_processing_key, user_id):
        return jsonify({'message': f'Usuário {user_id} removido do processamento'})
    
    return jsonify({'error': 'Usuário não encontrado no processamento'}), 404





if __name__ == '__main__':
    app.run(debug=True)
