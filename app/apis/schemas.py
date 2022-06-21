from marshmallow import Schema, fields

BigQueueMessageSchema = Schema.from_dict(
    {"steps": fields.List(fields.Int(), required=True)}
)

BigQueueBodySchema = Schema.from_dict(
    {
        "topic": fields.Str(),
        "cluster": fields.Str(),
        "consumer": fields.Str(),
        "id": fields.Str(),
        "msg": fields.Nested(BigQueueMessageSchema, required=True),
        "publish_time": fields.Raw(),
        "filters": fields.List(fields.Str(), allow_none=True),
        "uid": fields.Str(),
        "recipientCallback": fields.Str(),
    }
)


FuryJobBodySchema = Schema.from_dict(
    {
        "execution_id": fields.Raw(),
        "process_name": fields.Raw(),
        "job_name": fields.Raw(),
    }
)

AuthSchema = Schema.from_dict({"username": fields.Str(), "password": fields.Str()})
