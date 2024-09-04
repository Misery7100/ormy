# import unittest
# from unittest.mock import MagicMock, patch

# from ormwtf.base.abc import DocumentOrmABC


# # ----------------------- #
# # Create a concrete subclass for testing
# class ConcreteDocumentOrm(DocumentOrmABC):
#     @classmethod
#     def create(cls, data):
#         return cls()

#     @classmethod
#     async def acreate(cls, data):
#         return cls()

#     def save(self):
#         return self

#     async def asave(self):
#         return self

#     @classmethod
#     def find(cls, id_, *args, **kwargs):
#         return cls()

#     @classmethod
#     async def afind(cls, id_, *args, **kwargs):
#         return cls()


# # ----------------------- #


# class TestDocumentOrmABC(unittest.TestCase):
#     def setUp(self):
#         self.document_class = ConcreteDocumentOrm
#         self.document_instance = self.document_class()

#     # ....................... #

#     @patch.object(ConcreteDocumentOrm, "create")
#     def test_create_method(self, mock):
#         data = MagicMock()
#         self.document_class.create(data)
#         mock.assert_called_once_with(data)

#     # ....................... #

#     @patch.object(ConcreteDocumentOrm, "save")
#     def test_save_method(self, mock):
#         self.document_instance.save()
#         mock.assert_called_once()

#     # ....................... #

#     @patch.object(ConcreteDocumentOrm, "find")
#     def test_find_method(self, mock):
#         id_ = MagicMock()
#         self.document_class.find(id_)
#         mock.assert_called_once_with(id_)


# # ----------------------- #


# class TestDocumentOrmABCAsync(unittest.IsolatedAsyncioTestCase):
#     def setUp(self):
#         self.document_class = ConcreteDocumentOrm
#         self.document_instance = self.document_class()

#     # ....................... #

#     @patch.object(ConcreteDocumentOrm, "acreate")
#     async def test_acreate_method(self, mock):
#         data = MagicMock()
#         await self.document_class.acreate(data)
#         mock.assert_called_once_with(data)

#     # ....................... #

#     @patch.object(ConcreteDocumentOrm, "asave")
#     async def test_asave_method(self, mock):
#         await self.document_instance.asave()
#         mock.assert_called_once()

#     # ....................... #

#     @patch.object(ConcreteDocumentOrm, "afind")
#     async def test_afind_method(self, mock):
#         id_ = MagicMock()
#         await self.document_class.afind(id_)
#         mock.assert_called_once_with(id_)


# # ----------------------- #

# if __name__ == "__main__":
#     unittest.main()
