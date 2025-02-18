# from contextlib import asynccontextmanager, contextmanager
# from typing import (
#     Any,
#     Callable,
#     Dict,
#     List,
#     Optional,
#     Tuple,
#     Type,
#     TypeVar,
#     Union,
#     cast,
# )

# from firebase_admin import firestore, firestore_async  # type: ignore
# from google.api_core.retry import AsyncRetry, Retry
# from google.cloud.firestore_v1 import (
#     AsyncCollectionReference,
#     AsyncDocumentReference,
#     AsyncQuery,
#     AsyncTransaction,
#     AsyncWriteBatch,
#     CollectionReference,
#     DocumentReference,
#     FieldFilter,
#     Query,
#     Transaction,
#     WriteBatch,
# )
# from google.cloud.firestore_v1.aggregation import AggregationQuery
# from google.cloud.firestore_v1.async_aggregation import AsyncAggregationQuery
# from google.cloud.firestore_v1.transforms import (
#     ArrayRemove,
#     ArrayUnion,
#     Increment,
#     Maximum,
#     Minimum,
#     Sentinel,
# )

# from ormy.base.abc import ConfigABC, DocumentABC
# from ormy.base.typing import DocumentID

# from .config import FirestoreConfig

# # ----------------------- #

# T = TypeVar("T", bound="FirestoreBase")
# C = TypeVar("C", bound="ConfigABC")

# # ! ???
# FsTransform = Union[Sentinel, ArrayRemove, ArrayUnion, Increment, Maximum, Minimum]

# # ----------------------- #


# class FirestoreBase(DocumentABC):  # TODO: add docstrings

#     configs = [FirestoreConfig()]
#     _registry = {FirestoreConfig: {}}

#     # ....................... #

#     def __init_subclass__(cls: Type[T], **kwargs):
#         super().__init_subclass__(**kwargs)

#         cls._firestore_register_subclass()
#         cls._merge_registry()

#         FirestoreBase._registry = cls._merge_registry_helper(
#             FirestoreBase._registry,
#             cls._registry,
#         )

#     # ....................... #

#     @classmethod
#     def _firestore_register_subclass(cls: Type[T]):
#         """Register subclass in the registry"""

#         cfg = cls.get_config(type_=FirestoreConfig)
#         db = cfg.database
#         col = cfg.collection

#         # TODO: use exact default value from class
#         if cfg.include_to_registry and not cfg.is_default():
#             cls._registry[FirestoreConfig] = cls._registry.get(FirestoreConfig, {})
#             cls._registry[FirestoreConfig][db] = cls._registry[FirestoreConfig].get(
#                 db, {}
#             )
#             cls._registry[FirestoreConfig][db][col] = cls

#     # ....................... #

#     @classmethod
#     @contextmanager
#     def _client(cls: Type[T]):
#         """Get syncronous Firestore client"""

#         cfg = cls.get_config(type_=FirestoreConfig)

#         project_id = cfg.credentials.project_id
#         app = cfg.credentials.app
#         database = cfg.database

#         client = firestore.client(app)
#         client._database_string_internal = f"projects/{project_id}/databases/{database}"

#         try:
#             yield client

#         finally:
#             client.close()

#     # ....................... #

#     @classmethod
#     @asynccontextmanager
#     async def _aclient(cls: Type[T]):
#         """Get asyncronous Firestore client"""

#         cfg = cls.get_config(type_=FirestoreConfig)

#         project_id = cfg.credentials.project_id
#         app = cfg.credentials.app
#         database = cfg.database

#         client = firestore_async.client(app)
#         client._database_string_internal = f"projects/{project_id}/databases/{database}"

#         try:
#             yield client

#         finally:
#             client.close()

#     # ....................... #

#     @classmethod
#     def _batch(cls: Type[T]) -> WriteBatch:
#         """
#         ...
#         """

#         with cls._client() as client:
#             return client.batch()

#     # ....................... #

#     @classmethod
#     async def _abatch(cls: Type[T]) -> AsyncWriteBatch:
#         """
#         ...
#         """

#         async with cls._aclient() as client:
#             return client.batch()

#     # ....................... #

#     @classmethod
#     def _get_collection(cls: Type[T]) -> CollectionReference:
#         """Get assigned Firestore collection in syncronous mode"""

#         cfg = cls.get_config(type_=FirestoreConfig)

#         with cls._client() as client:
#             return client.collection(cfg.collection)

#     # ....................... #

#     @classmethod
#     async def _aget_collection(cls: Type[T]) -> AsyncCollectionReference:
#         """Get assigned Firestore collection in asyncronous mode"""

#         cfg = cls.get_config(type_=FirestoreConfig)

#         async with cls._aclient() as client:
#             return client.collection(cfg.collection)

#     # ....................... #

#     @classmethod
#     def _ref(cls: Type[T], id_: DocumentID) -> DocumentReference:
#         """
#         Get a document reference from assigned collection in syncronous mode
#         """

#         collection = cls._get_collection()
#         _id = str(id_) if id_ is not None else None
#         ref = collection.document(_id)

#         return ref

#     # ....................... #

#     @classmethod
#     async def _aref(cls: Type[T], id_: DocumentID) -> AsyncDocumentReference:
#         """
#         Get a document reference from assigned collection in asyncronous mode
#         """

#         collection = await cls._aget_collection()
#         _id = str(id_) if id_ is not None else None
#         ref = collection.document(_id)  # type: ignore

#         return ref

#     # ....................... #

#     @classmethod
#     def create(cls: Type[T], data: T) -> T:
#         """
#         ...
#         """

#         document = data.model_dump()
#         _id: DocumentID = document["id"]
#         ref = cls._ref(_id)
#         snapshot = ref.get()

#         if snapshot.exists:
#             raise ValueError(f"Document with ID {_id} already exists")

#         ref.set(document)

#         return data

#     # ....................... #

#     @classmethod
#     async def acreate(cls: Type[T], data: T) -> T:
#         """
#         ...
#         """

#         document = data.model_dump()
#         _id: DocumentID = document["id"]
#         ref = await cls._aref(_id)
#         snapshot = await ref.get()

#         if snapshot.exists:
#             raise ValueError(f"Document with ID {_id} already exists")

#         await ref.set(document)

#         return data

#     # ....................... #

#     @classmethod
#     @contextmanager
#     def transaction(cls: Type[T]):
#         """
#         ...
#         """

#         with cls._client() as client:
#             try:
#                 t = client.transaction()
#                 t._begin()

#                 yield t

#             finally:
#                 t._commit()

#     # ....................... #

#     @classmethod
#     @contextmanager
#     def raw_transaction(cls: Type[T]):
#         """
#         ...
#         """

#         with cls._client() as client:
#             try:
#                 t = client.transaction()

#                 yield t

#             finally:
#                 pass

#     # ....................... #

#     @classmethod
#     @asynccontextmanager
#     async def atransaction(cls: Type[T]):
#         """
#         ...
#         """

#         async with cls._aclient() as client:
#             try:
#                 t = client.transaction()
#                 await t._begin()

#                 yield t

#             finally:
#                 await t._commit()

#     # ....................... #

#     def save(self: T) -> T:
#         """
#         ...
#         """

#         document = self.model_dump()
#         _id: DocumentID = document["id"]
#         ref = self._ref(_id)
#         ref.set(document)

#         return self

#     # ....................... #

#     async def asave(self: T) -> T:
#         """
#         ...
#         """

#         document = self.model_dump()
#         _id: DocumentID = document["id"]
#         ref = await self._aref(_id)
#         await ref.set(document)

#         return self

#     # ....................... #

#     def update_in_transaction(
#         self: T,
#         updates: Dict[str, Any],
#         transaction: Transaction,
#     ):
#         """
#         ...
#         """

#         data_filtered = {k: v for k, v in updates.items() if hasattr(self, k)}
#         ref = self._ref(self.id)
#         transaction.update(ref, data_filtered)

#     # ....................... #

#     async def aupdate_in_transaction(
#         self: T,
#         updates: Dict[str, Any],
#         transaction: AsyncTransaction,
#     ):
#         """
#         ...
#         """

#         data_filtered = {k: v for k, v in updates.items() if hasattr(self, k)}
#         ref = await self._aref(self.id)
#         transaction.update(ref, data_filtered)

#     # ....................... #

#     def atmoic_update(self: T, updates: Dict[str, Any]):
#         """
#         ...
#         """

#         data_filtered = {k: v for k, v in updates.items() if hasattr(self, k)}
#         ref = self._ref(self.id)
#         ref.update(data_filtered)

#     # ....................... #

#     async def aatomic_update(self: T, updates: Dict[str, Any]):
#         """
#         ...
#         """

#         data_filtered = {k: v for k, v in updates.items() if hasattr(self, k)}
#         ref = await self._aref(self.id)
#         await ref.update(data_filtered)

#     # ....................... #

#     #! Do we need to retrieve documents?

#     @classmethod
#     def create_many(
#         cls: Type[T],
#         data: List[T],
#         autosave: bool = True,
#         bypass: bool = False,
#     ) -> WriteBatch:
#         """
#         ...
#         """

#         batch = cls._batch()

#         for x in data:
#             document = x.model_dump()
#             _id: DocumentID = document["id"]
#             ref = cls._ref(_id)
#             snapshot = ref.get()

#             if snapshot.exists:
#                 if not bypass:
#                     raise ValueError(f"Document with ID {_id} already exists")
#             else:
#                 batch.set(ref, document)

#         if autosave:
#             batch.commit()

#         return batch

#     # ....................... #

#     #! Do we need to retrieve documents?

#     @classmethod
#     async def acreate_many(
#         cls: Type[T],
#         data: List[T],
#         autosave: bool = True,
#         bypass: bool = False,
#     ) -> AsyncWriteBatch:
#         """
#         ...
#         """

#         batch = await cls._abatch()

#         for x in data:
#             document = x.model_dump()
#             _id: DocumentID = document["id"]
#             ref = await cls._aref(_id)
#             snapshot = await ref.get()

#             if snapshot.exists:
#                 if not bypass:
#                     raise ValueError(f"Document with ID {_id} already exists")

#             else:
#                 batch.set(ref, document)

#         if autosave:
#             await batch.commit()

#         return batch

#     # ....................... #

#     @classmethod
#     def update_many(
#         cls: Type[T],
#         data: List[T],
#         autosave: bool = True,
#     ) -> Optional[WriteBatch]:
#         """
#         ...
#         """

#         pass

#     # ....................... #

#     @classmethod
#     async def aupdate_many(
#         cls: Type[T],
#         data: List[T],
#         autosave: bool = True,
#     ) -> Optional[AsyncWriteBatch]:
#         """
#         ...
#         """

#         pass

#     # ....................... #

#     @classmethod
#     def find(
#         cls: Type[T],
#         id_: DocumentID,
#         bypass: bool = False,
#         transaction: Optional[Transaction] = None,
#         return_snapshot: bool = False,
#     ) -> Optional[T | Tuple[T, DocumentReference]]:
#         """
#         ...
#         """

#         ref = cls._ref(id_)

#         if transaction:
#             snapshot = ref.get(
#                 transaction=transaction,
#                 retry=Retry(),
#             )

#         else:
#             snapshot = ref.get()

#         if snapshot.exists:
#             if return_snapshot:
#                 return cls(**snapshot.to_dict()), snapshot  # type: ignore

#             return cls(**snapshot.to_dict())  # type: ignore

#         elif not bypass:
#             raise ValueError(f"Document with ID {id_} not found")

#         return None

#     # ....................... #

#     @classmethod
#     async def afind(
#         cls: Type[T],
#         id_: DocumentID,
#         bypass: bool = False,
#         transaction: Optional[Transaction] = None,
#         return_snapshot: bool = False,
#     ) -> Optional[T | Tuple[T, DocumentReference]]:
#         """
#         ...
#         """

#         ref = await cls._aref(id_)

#         if transaction:
#             snapshot = await ref.get(
#                 transaction=transaction,
#                 retry=AsyncRetry(),
#             )

#         else:
#             snapshot = await ref.get()

#         if snapshot.exists:
#             if return_snapshot:
#                 return cls(**snapshot.to_dict()), snapshot  # type: ignore

#             return cls(**snapshot.to_dict())  # type: ignore

#         elif not bypass:
#             raise ValueError(f"Document with ID {id_} not found")

#         return None

#     # ....................... #

#     #! TODO: Support transactions
#     @classmethod
#     def find_many(
#         cls: Type[T],
#         filters: Optional[List[FieldFilter]] = None,
#         limit: int = 100,
#         offset: int = 0,
#     ) -> List[T]:
#         """
#         ...
#         """

#         collection = cls._get_collection()
#         query = cast(Query, collection)

#         if filters:
#             for f in filters:
#                 query = query.where(filter=f)

#         query = query.limit(limit).offset(offset)
#         docs = query.get()

#         return [cls(**doc.to_dict()) for doc in docs]  # type: ignore

#     # ....................... #

#     #! TODO: Support transactions
#     @classmethod
#     async def afind_many(
#         cls: Type[T],
#         filters: Optional[List[FieldFilter]] = None,
#         limit: int = 100,
#         offset: int = 0,
#     ) -> List[T]:
#         """
#         ...
#         """

#         collection = await cls._aget_collection()
#         query = cast(AsyncQuery, collection)

#         if filters:
#             for f in filters:
#                 query = query.where(filter=f)

#         query = query.limit(limit).offset(offset)
#         docs = await query.get()

#         return [cls(**doc.to_dict()) for doc in docs]  # type: ignore

#     # ....................... #

#     @classmethod
#     def count(
#         cls: Type[T],
#         filters: Optional[List[FieldFilter]] = None,
#     ) -> int:
#         """
#         ...
#         """

#         collection = cls._get_collection()
#         query = cast(Query, collection)

#         if filters:
#             for f in filters:
#                 query = query.where(filter=f)

#         aq: AggregationQuery = query.count()  # type: ignore
#         res = aq.get()
#         number = int(res[0][0].value)  # type: ignore

#         return number

#     # ....................... #

#     @classmethod
#     async def acount(
#         cls: Type[T],
#         filters: Optional[List[FieldFilter]] = None,
#     ) -> int:
#         """
#         ...
#         """

#         collection = await cls._aget_collection()
#         query = cast(AsyncQuery, collection)

#         if filters:
#             for f in filters:
#                 query = query.where(filter=f)

#         aq: AsyncAggregationQuery = query.count()  # type: ignore
#         res = await aq.get()
#         number = int(res[0][0].value)  # type: ignore

#         return number

#     # ....................... #

#     @classmethod
#     def find_all(
#         cls: Type[T],
#         filters: Optional[List[FieldFilter]] = None,
#         batch_size: int = 100,
#     ) -> List[T]:
#         """
#         ...
#         """

#         cnt = cls.count(filters=filters)
#         found: List[T] = []

#         for j in range(0, cnt, batch_size):
#             docs = cls.find_many(filters=filters, limit=batch_size, offset=j)
#             found.extend(docs)

#         return found

#     # ....................... #

#     @classmethod
#     async def afind_all(
#         cls: Type[T],
#         filters: Optional[List[FieldFilter]] = None,
#         batch_size: int = 100,
#     ) -> List[T]:
#         """
#         ...
#         """

#         cnt = await cls.acount(filters=filters)
#         found: List[T] = []

#         for j in range(0, cnt, batch_size):
#             docs = await cls.afind_many(filters=filters, limit=batch_size, offset=j)
#             found.extend(docs)

#         return found

#     # ....................... #

#     #! TODO: Support transactions?

#     @classmethod
#     def stream(
#         cls: Type[T],
#         filters: Optional[List[FieldFilter]] = None,
#     ):
#         """
#         ...
#         """

#         collection = cls._get_collection()
#         query = cast(Query, collection)

#         if filters:
#             for f in filters:
#                 query = query.where(filter=f)

#         return query.stream()

#     # ....................... #

#     #! TODO: Support transactions?

#     @classmethod
#     async def astream(
#         cls: Type[T],
#         filters: Optional[List[FieldFilter]] = None,
#     ):
#         """
#         ...
#         """

#         collection = await cls._aget_collection()
#         query = cast(AsyncQuery, collection)

#         if filters:
#             for f in filters:
#                 query = query.where(filter=f)

#         return query.stream()

#     # ....................... #

#     @classmethod
#     def on_snapshot(
#         cls: Type[T],
#         callback: Callable,
#     ):
#         """
#         _summary_

#         Args:
#             callback (Callable): _description_
#         """

#         collection = cls._get_collection()
#         return collection.on_snapshot(callback)
