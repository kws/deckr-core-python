import anyio
import pytest

from deckr.core.util.owned_tasks import OwnedTaskGroup


@pytest.mark.asyncio
async def test_worker_group_rejects_access_before_start_and_double_start():
    async with anyio.create_task_group() as parent:
        owner = OwnedTaskGroup(parent, not_started_message="runtime not started")
        with pytest.raises(RuntimeError, match="not started"):
            _ = owner.group
        group = await owner.start()
        assert owner.group is group
        with pytest.raises(RuntimeError, match="already started"):
            await owner.start()
        await owner.aclose()
        with pytest.raises(RuntimeError, match="not started"):
            _ = owner.group
