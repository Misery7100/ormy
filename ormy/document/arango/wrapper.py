from typing import Any, ClassVar, Literal, Optional, Self, Sequence, cast

from ormy.exceptions import BadRequest, Conflict, ModuleNotFound, NotFound

try:
    from arango.client import ArangoClient
    from arango.cursor import Cursor
except ImportError as e:
    raise ModuleNotFound(extra="arango", packages=["python-arango"]) from e

from ormy.base.generic import TabularData
from ormy.document._abc import DocumentABC

from .config import ArangoConfig

# ----------------------- #


class ArangoBase(DocumentABC):
    """ArangoDB base class"""

    config: ClassVar[ArangoConfig] = ArangoConfig()

    __static: ClassVar[Optional[ArangoClient]] = None

    # ....................... #

    def __init_subclass__(cls, **kwargs):
        """Initialize subclass"""

        super().__init_subclass__(**kwargs)

        cls._register_subclass_helper(discriminator=["database", "collection"])

    # ....................... #

    @classmethod
    def __client(cls):
        """
        Get syncronous ArangoDB client

        Returns:
            client (arango.ArangoClient): Syncronous ArangoDB client
        """

        if cls.__static is None:
            cls.__static = ArangoClient(hosts=cls.config.url())

        return cls.__static

    # ....................... #

    @classmethod
    def _get_database(cls):
        """
        Get assigned ArangoDB database

        Returns:
            database (arango.Database): Assigned ArangoDB database
        """

        client = cls.__client()
        username = cls.config.credentials.username.get_secret_value()
        password = cls.config.credentials.password.get_secret_value()
        database = cls.config.database

        sys_db = client.db("_system", username=username, password=password)

        if not sys_db.has_database(database):
            sys_db.create_database(database)

        db = client.db(database, username=username, password=password)

        return db

    # ....................... #

    @classmethod
    def _aget_database(cls):
        """
        Get assigned ArangoDB database in asyncronous mode

        Returns:
            database (arango.AsyncDatabase): Assigned ArangoDB database
        """

        db = cls._get_database()

        return db.begin_async_execution(return_result=True)

    # ....................... #

    @classmethod
    def _get_collection(cls):
        """
        Get assigned ArangoDB collection

        Returns:
            collection (arango.Collection): Assigned ArangoDB collection
        """

        collection = cls.config.collection
        db = cls._get_database()

        if not db.has_collection(collection):
            db.create_collection(collection)

        return db.collection(collection)

    # ....................... #

    @classmethod
    def create(cls, data: Self):
        """
        Create a new document in the collection

        Args:
            data (ArangoBase): Data model to be created

        Returns:
            res (ArangoBase): Created data model

        Raises:
            Conflict: Document already exists
        """

        collection = cls._get_collection()
        document = data.model_dump()

        _id = str(document["id"])

        if collection.has(_id):
            raise Conflict("Document already exists")

        collection.insert({**document, "_key": _id})

        return data

    # ....................... #

    def save(self: Self):
        """
        Save a document in the collection.
        Document will be updated if exists

        Returns:
            self (MongoBase): Saved data model
        """

        collection = self._get_collection()
        document = self.model_dump()

        _id = str(document["id"])

        if collection.has(_id):
            collection.replace({**document, "_key": _id}, silent=True)

        else:
            collection.insert({**document, "_key": _id})

        return self

    # ....................... #

    @classmethod
    def create_many(cls, data: list[Self]):
        """
        Create multiple documents in the collection

        Args:
            data (List[ArangoBase]): List of data models to be created

        Returns:
            res (List[ArangoBase]): List of created data models
        """

        collection = cls._get_collection()
        _data = [item.model_dump() for item in data]
        _data = [{"_key": d["id"], **d} for d in _data]

        res = collection.insert_many(_data)

        successful_docs = [x for x in res if isinstance(x, dict)]  # type: ignore
        successful_keys = [x["_key"] for x in successful_docs]

        return [d for d in data if d.id in successful_keys]

    # ....................... #

    @classmethod
    def update_many(cls, data: list[Self]):
        raise NotImplementedError

    # ....................... #

    @classmethod
    def find(cls, id_: str):
        """
        Find a document in the collection

        Args:
            id_ (DocumentID, optional): Document ID

        Returns:
            res (ArangoBase): Found data model

        Raises:
            BadRequest: Request or value is required
            NotFound: Document not found
        """

        collection = cls._get_collection()

        request = {"_key": id_}

        document = collection.get(request)
        document = cast(dict | None, document)

        if not document:
            raise NotFound(f"Document with ID {id_} not found")

        return cls(**document)

    # ....................... #

    @classmethod
    def count(
        cls,
        query: Optional[str] = None,
        bind_vars: dict[str, Any] = {},
        doc_clause: str = "doc",
    ):
        """
        Count documents in the collection

        Args:
            query (Optional[str], optional): AQL query to count the documents.
            bind_vars (dict[str, Any], optional): Bind variables for the query.
            doc_clause (str, optional): Document clause substitution.

        Returns:
            res (int): Number of documents
        """

        if query is None:
            collection = cls._get_collection()
            cnt = collection.count()
            cnt = cast(int, cnt)

        else:
            db = cls._get_database()
            q = f"""
            RETURN LENGTH(
                FOR {doc_clause} IN {cls.config.collection}
                    {query}
                    RETURN {doc_clause}
            )
            """
            cursor = db.aql.execute(
                query=q,
                bind_vars=bind_vars,
            )
            cursor = cast(Cursor, cursor)
            cnt = next(cursor)
            cnt = cast(int, cnt)

        return cnt

    # ....................... #

    @classmethod
    def find_many(
        cls,
        query: str,
        bind_vars: dict[str, Any] = {},
        limit: int = 100,
        offset: int = 0,
        doc_clause: str = "doc",
    ):
        """
        Find multiple documents in the collection matching the query

        Args:
            query (str): AQL query to find the documents.
            bind_vars (dict[str, Any], optional): Bind variables for the query.
            limit (int, optional): Limit the number of documents.
            offset (int, optional): Offset the number of documents.
            doc_clause (str, optional): Document clause substitution.

        Returns:
            res (list[ArangoBase]): List of found data models
        """

        from arango.cursor import Cursor

        db = cls._get_database()

        q = f"""
        FOR {doc_clause} IN {cls.config.collection}
            {query}
            LIMIT {offset}, {limit}
            RETURN {doc_clause}
        """

        cursor = db.aql.execute(
            query=q,
            bind_vars=bind_vars,
        )
        cursor = cast(Cursor, cursor)
        res = [cls(**doc) for doc in cursor]

        return res

    # ....................... #

    @classmethod
    def find_all(
        cls,
        query: str,
        bind_vars: dict[str, Any] = {},
        batch_size: int = 1000,
        doc_clause: str = "doc",
    ):
        """
        Find all documents in the collection matching the query

        Args:
            query (str): AQL query to find the documents.
            bind_vars (dict[str, Any], optional): Bind variables for the query.
            batch_size (int, optional): Batch size.
            doc_clause (str, optional): Document clause substitution.

        Returns:
            res (list[ArangoBase]): List of found data models
        """

        from arango.cursor import Cursor

        db = cls._get_database()

        q = f"""
        FOR {doc_clause} IN {cls.config.collection}
            {query}
            RETURN {doc_clause}
        """

        cursor = db.aql.execute(
            query=q,
            bind_vars=bind_vars,
            batch_size=batch_size,
        )
        cursor = cast(Cursor, cursor)
        res = [cls(**doc) for doc in cursor]

        return res

    # ....................... #

    @classmethod
    def patch(
        cls,
        data: TabularData,
        include: Optional[Sequence[str]] = None,
        on: Optional[str] = None,
        left_on: Optional[str] = None,
        right_on: Optional[str] = None,
        prefix: Optional[str] = None,
        kind: Literal["inner", "left"] = "inner",
        fill_none: Any = None,
    ):
        """
        Extend data with documents from the collection

        Args:
            data (TabularData): Data to be extended
            include (Sequence[str], optional): Fields to include
            on (str, optional): Field to join on
            left_on (str, optional): Field to join on the left
            right_on (str, optional): Field to join on the right
            prefix (str, optional): Prefix for the fields
            kind (Literal["inner", "left"], optional): Kind of join
            fill_none (Any, optional): Value to fill None

        Returns:
            res (TabularData): Extended data

        Raises:
            BadRequest: `data` is required
            BadRequest: Fields `left_on` and `right_on` are required
        """

        if not data:
            raise BadRequest("`data` is required")

        if on is not None:
            left_on = on
            right_on = on

        if left_on is None or right_on is None:
            raise BadRequest("Fields `left_on` and `right_on` are required")

        if kind == "left" and not include:  # type safe
            raise BadRequest("Fields to include are required for left join")

        docs = cls.find_all(
            query=f"FILTER {right_on} IN @left_on_unique",
            bind_vars={
                "left_on_unique": list(data.unique(left_on)),
            },
        )
        tab_docs = TabularData(docs)

        if include is not None:
            include = list(include)
            include.append(right_on)
            include = list(set(include))

        if not len(tab_docs) and kind == "left":
            tab_docs = TabularData([{k: fill_none for k in include}])  # type: ignore

        return data.join(
            other=tab_docs.slice(include=include),
            on=on,
            left_on=left_on,
            right_on=right_on,
            prefix=prefix,
            kind=kind,
            fill_none=fill_none,
        )
