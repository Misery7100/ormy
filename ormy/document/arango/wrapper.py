from typing import Any, ClassVar, Literal, Optional, Self, Sequence, cast

from pydantic import BaseModel

from ormy.exceptions import BadRequest, Conflict, ModuleNotFound, NotFound

try:
    from arango.client import ArangoClient
    from arango.cursor import Cursor
except ImportError as e:
    raise ModuleNotFound(extra="arango", packages=["python-arango"]) from e

from ormy._abc import AbstractABC
from ormy._abc.registry import Registry
from ormy.base.generic import TabularData
from ormy.base.pydantic import TrimDocMixin
from ormy.document._abc import SyncDocumentABC

from .config import ArangoConfig, ArangoGraphConfig

# ----------------------- #


class ArangoBase(SyncDocumentABC, TrimDocMixin):
    """ArangoDB base class"""

    config: ClassVar[ArangoConfig] = ArangoConfig()
    _static: ClassVar[Optional[ArangoClient]] = None
    __discriminator__ = ["database", "collection"]

    # ....................... #

    @classmethod
    def _client(cls):
        """
        Get syncronous ArangoDB client

        Returns:
            client (arango.ArangoClient): Syncronous ArangoDB client
        """

        if cls._static is None:
            cls._static = ArangoClient(hosts=cls.config.url())

        return cls._static

    # ....................... #

    @classmethod
    def _get_database(cls):
        """
        Get assigned ArangoDB database

        Returns:
            database (arango.StandardDatabase): Assigned ArangoDB database
        """

        client = cls._client()
        username = cls.config.credentials.username.get_secret_value()
        password = cls.config.credentials.password.get_secret_value()
        database = cls.config.database

        sys_db = client.db(
            "_system",
            username=username,
            password=password,
        )

        if not sys_db.has_database(database):
            sys_db.create_database(database)

        db = client.db(
            database,
            username=username,
            password=password,
        )

        return db

    # ....................... #

    @classmethod
    def _get_collection(cls):
        """
        Get assigned ArangoDB collection

        Returns:
            collection (arango.StandardCollection): Assigned ArangoDB collection
        """

        collection = cls.config.collection
        db = cls._get_database()

        if not db.has_collection(collection):
            db.create_collection(collection)

        return db.collection(collection)

    # ....................... #

    @staticmethod
    def _serialize(doc: dict) -> dict:
        """
        Serialize a document

        Args:
            doc (dict): Document to serialize

        Returns:
            doc (dict): Serialized document
        """

        doc["_key"] = doc["id"]

        return doc

    # ....................... #

    @staticmethod
    def _deserialize(doc: dict) -> dict:
        """
        Deserialize a document

        Args:
            doc (dict): Document to deserialize

        Returns:
            doc (dict): Deserialized document
        """

        return doc

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
        document = cls._serialize(data.model_dump())

        if collection.has(document["_key"]):
            raise Conflict("Document already exists")

        collection.insert(document)

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
        document = self._serialize(self.model_dump())

        if collection.has(document["_key"]):
            collection.replace(document, silent=True)

        else:
            collection.insert(document)

        return self

    # ....................... #

    @classmethod
    def create_many(cls, data: list[Self]):
        """
        Create multiple documents in the collection

        Args:
            data (list[ArangoBase]): List of data models to be created

        Returns:
            res (list[ArangoBase]): List of created data models
        """

        collection = cls._get_collection()
        _data = [cls._serialize(item.model_dump()) for item in data]

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

        return cls(**cls._deserialize(document))

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
        res = [cls(**cls._deserialize(doc)) for doc in cursor]

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
        res = [cls(**cls._deserialize(doc)) for doc in cursor]

        return res

    # ....................... #

    def kill(self: Self):
        """
        Hard delete a document from the collection
        """

        collection = self._get_collection()

        request = {"_key": self.id}
        collection.delete(request)

    # ....................... #

    @classmethod
    def kill_many(cls, ids: list[str]):
        """
        Hard delete multiple documents from the collection
        """

        collection = cls._get_collection()
        request = [{"_key": id_} for id_ in ids]
        collection.delete_many(documents=request)

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

    # ....................... #

    @property
    def global_id(self: Self) -> str:
        """
        Get global ID

        Returns:
            global_id (str): Global ID
        """

        collection = self.config.collection

        return f"{collection}/{self.id}"

    # ....................... #

    @staticmethod
    def registry_helper_safe_create_collections():
        """Safe create or update collections"""

        entries: list[ArangoBase] = Registry.get_by_config(ArangoConfig)

        for x in entries:
            x._get_collection()


# ....................... #


class ArangoBaseEdge(ArangoBase):
    """ArangoDB base edge class"""

    from_: Optional[str] = None
    to_: Optional[str] = None
    __discriminator__ = ["database", "collection"]

    # ....................... #

    @classmethod
    def _get_collection(cls):
        """
        Get assigned ArangoDB collection

        Returns:
            collection (arango.StandardCollection): Assigned ArangoDB collection
        """

        collection = cls.config.collection
        db = cls._get_database()

        if not db.has_collection(collection):
            db.create_collection(collection, edge=True)

        return db.collection(collection)

    # ....................... #

    @staticmethod
    def _serialize(doc: dict) -> dict:
        """
        Serialize an edge document

        Args:
            doc (dict): Edge document to serialize

        Returns:
            doc (dict): Serialized edge document
        """

        doc = ArangoBase._serialize(doc)

        doc["_from"] = doc.pop("from_")
        doc["_to"] = doc.pop("to_")

        return doc

    # ....................... #

    @staticmethod
    def _deserialize(doc: dict) -> dict:
        """
        Deserialize an edge document

        Args:
            doc (dict): Edge document to deserialize

        Returns:
            doc (dict): Deserialized edge document
        """

        doc = ArangoBase._deserialize(doc)

        doc["from_"] = doc.pop("_from")
        doc["to_"] = doc.pop("_to")

        return doc

    # ....................... #

    @classmethod
    def create(cls, data: Self):
        """
        Create a new document in the collection

        Args:
            data (ArangoBaseEdge): Data model to be created

        Returns:
            res (ArangoBaseEdge): Created data model

        Raises:
            Conflict: Document already exists
        """

        collection = cls._get_collection()
        document = cls._serialize(data.model_dump())

        if collection.has({"_from": document["_from"], "_to": document["_to"]}):
            raise Conflict("Edge document already exists")

        collection.insert(document)

        return data

    # ....................... #

    def save(self: Self):
        """
        Save a document in the collection.
        Document will be updated if exists

        Returns:
            self (ArangoBaseEdge): Saved data model
        """

        collection = self._get_collection()
        document = self._serialize(self.model_dump())

        if collection.has(document["_key"]):  # ? Use _from and _to instead?
            collection.replace(document, silent=True)

        else:
            collection.insert(document)

        return self

    # ....................... #

    @classmethod
    def create_many(cls, data: list[Self]):
        """
        Create multiple documents in the collection

        Args:
            data (list[ArangoBase]): List of data models to be created

        Returns:
            res (list[ArangoBase]): List of created data models
        """

        collection = cls._get_collection()

        _data = [cls._serialize(item.model_dump()) for item in data]
        res = collection.insert_many(_data)

        successful_docs = [x for x in res if isinstance(x, dict)]  # type: ignore
        successful_keys = [x["_key"] for x in successful_docs]

        return [d for d in data if d.id in successful_keys]

    # ....................... #

    @classmethod
    def find(cls, id_: str):
        raise NotImplementedError

    # ....................... #

    @classmethod
    def find_by_vertices(cls, from_: str, to_: str):
        """
        Find an edge document in the collection

        Args:
            from_ (DocumentID): From node ID
            to_ (DocumentID): To node ID

        Returns:
            res (ArangoBase): Found data model

        Raises:
            NotFound: Edge not found
        """

        collection = cls._get_collection()

        request = {"_from": from_, "_to": to_}

        document = collection.get(request)
        document = cast(dict | None, document)

        if not document:
            raise NotFound(f"Edge {from_} -> {to_} not found")

        return cls(**cls._deserialize(document))

    # ....................... #

    @classmethod
    def patch(cls):
        raise NotImplementedError


# ....................... #


class ArangoEdgeDefinition(BaseModel):
    """ArangoDB edge definition"""

    edge_collection: ArangoBaseEdge
    from_nodes: list[ArangoBase]
    to_nodes: list[ArangoBase]


# ....................... #


class ArangoGraph(AbstractABC):
    """ArangoDB graph class"""

    edge_definitions: ClassVar[list[ArangoEdgeDefinition]] = []

    # ....................... #

    config: ClassVar[ArangoGraphConfig] = ArangoGraphConfig()
    _static: ClassVar[Optional[ArangoClient]] = None
    __discriminator__ = ["database", "name"]

    # ....................... #

    @classmethod
    def _client(cls):
        """
        Get syncronous ArangoDB client

        Returns:
            client (arango.ArangoClient): Syncronous ArangoDB client
        """

        if cls._static is None:
            cls._static = ArangoClient(hosts=cls.config.url())

        return cls._static

    # ....................... #

    @classmethod
    def _get_database(cls):
        """
        Get assigned ArangoDB database

        Returns:
            database (arango.StandardDatabase): Assigned ArangoDB database
        """

        client = cls._client()
        username = cls.config.credentials.username.get_secret_value()
        password = cls.config.credentials.password.get_secret_value()
        database = cls.config.database

        sys_db = client.db(
            "_system",
            username=username,
            password=password,
        )

        if not sys_db.has_database(database):
            sys_db.create_database(database)

        db = client.db(
            database,
            username=username,
            password=password,
        )

        return db

    # ....................... #

    @classmethod
    def _get_graph(cls):
        """
        Get assigned ArangoDB graph

        Returns:
            graph (arango.StandardGraph): Assigned ArangoDB graph
        """

        name = cls.config.name
        db = cls._get_database()

        if not db.has_graph(name):
            edge_definitions = [
                {
                    "edge_collection": e.edge_collection.config.collection,
                    "from_vertex_collections": [
                        node.config.collection for node in e.from_nodes
                    ],
                    "to_vertex_collections": [
                        node.config.collection for node in e.to_nodes
                    ],
                }
                for e in cls.edge_definitions
            ]
            db.create_graph(name, edge_definitions=edge_definitions)

        return db.graph(name)

    # ....................... #

    @classmethod
    def raw_query(cls, query: str, bind_vars: dict[str, Any] = {}):
        """
        Query the graph

        Args:
            query (str): AQL query
            bind_vars (dict[str, Any], optional): Bind variables

        Returns:
            res (list[dict]): List of results
        """

        db = cls._get_database()

        res = db.aql.execute(query, bind_vars=bind_vars)

        return list(res)  #! What about heavy queries? !#

    # ....................... #

    @staticmethod
    def registry_helper_safe_create_graphs():
        """Safe create or update graphs"""

        entries: list[ArangoGraph] = Registry.get_by_config(ArangoGraphConfig)

        for x in entries:
            x._get_graph()
